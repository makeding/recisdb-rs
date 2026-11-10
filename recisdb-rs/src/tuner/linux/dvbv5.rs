use crate::channels::output::DvbFreq;
use crate::channels::{Channel, ChannelType};
use crate::tuner::Voltage;
use dvbv5::{DmxFd, FrontendId, FrontendParametersPtr};
use dvbv5_sys::fe_delivery_system::{SYS_ISDBS, SYS_ISDBT};
use dvbv5_sys::fe_status::FE_HAS_LOCK;
use dvbv5_sys::{
    dmx_output, dmx_ts_pes, dvb_set_compat_delivery_system, DTV_BANDWIDTH_HZ, DTV_FREQUENCY,
    DTV_ISDBT_LAYER_ENABLED, DTV_ISDBT_PARTIAL_RECEPTION, DTV_ISDBT_SOUND_BROADCASTING, DTV_STATUS,
    DTV_STAT_CNR, DTV_STREAM_ID, NO_STREAM_ID_FILTER,
};
use futures_util::io::BufReader;
use futures_util::{AsyncBufRead, AsyncRead};
use log::{info, warn};
use std::ffi::c_uint;
use std::io::Error;
use std::os::unix::fs::OpenOptionsExt;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::threaded_reader::ThreadedReader;

pub struct UnTunedTuner {
    id: (u8, u8),
    frontend: FrontendParametersPtr,
    pub buf_sz: usize,
}

impl UnTunedTuner {
    pub fn new(adapter_number: u8, fe_number: u8, buf_sz: usize) -> Result<Self, Error> {
        let frontend_id = FrontendId {
            adapter_number,
            frontend_number: fe_number,
        };
        let frontend = FrontendParametersPtr::new(
            &frontend_id,
            Some(if cfg!(debug_assertions) { 1 } else { 0 }),
            Some(false),
        )
        .map_err(|_| {
            Error::new(
                std::io::ErrorKind::Other,
                "Failed to open the DVB frontend device",
            )
        })?;

        Ok(Self {
            id: (adapter_number, fe_number),
            frontend,
            buf_sz,
        })
    }

    fn configure_frontend(
        &self,
        ch: &Channel,
        lnb: Option<&Voltage>,
        clear_unset_lnb: bool,
    ) -> Result<(), Error> {
        if matches!(&ch.ch_type, ChannelType::ISDBC(..) | ChannelType::J83B(..)) {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                "ISDB-C/J.83B is not supported with DVBv5. Use a supported px4_drv character device.",
            ));
        }
        const WAIT_DUR: std::time::Duration = std::time::Duration::from_secs(1);

        let raw_freq = DvbFreq::try_from(ch.ch_type.clone())?;
        unsafe {
            let p = self.frontend.get_c_ptr();
            let system = match &ch.ch_type {
                ChannelType::Terrestrial(..) | ChannelType::Catv(..) => SYS_ISDBT,
                ChannelType::BS(..) | ChannelType::CS(..) => SYS_ISDBS,
                _ => {
                    return Err(Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Unsupported DVB channel",
                    ))
                }
            };
            // S3-capable drivers use the same SYS_ISDBS ABI as recdvb.
            check_dvb_result(
                "select delivery system",
                dvb_set_compat_delivery_system(p, system as u32),
            )?;
            let store = |key, value| {
                check_dvb_result(
                    &format!("set DVB property {key}"),
                    dvbv5_sys::dvb_fe_store_parm(p, key, value),
                )
            };
            store(DTV_FREQUENCY as c_uint, raw_freq.freq_hz)?;
            match &ch.ch_type {
                ChannelType::Terrestrial(..) | ChannelType::Catv(..) => {
                    store(DTV_BANDWIDTH_HZ as c_uint, 6_000_000)?;
                    store(DTV_ISDBT_PARTIAL_RECEPTION, 0)?;
                    store(DTV_ISDBT_SOUND_BROADCASTING, 0)?;
                    store(DTV_ISDBT_LAYER_ENABLED, 0x07)?;
                }
                _ => {
                    store(
                        DTV_STREAM_ID as c_uint,
                        raw_freq.stream_id.unwrap_or(NO_STREAM_ID_FILTER as u32),
                    )?;
                }
            }

            // libdvbv5 drops DTV_VOLTAGE for ISDB-S; use FE_SET_VOLTAGE
            // directly.  A retained frontend explicitly turns it off when it
            // was not requested, so satellite power cannot carry into the
            // next terrestrial or unpowered session.
            let lnb_result = match lnb {
                Some(Voltage::_11v) => Some((dvbv5_sys::dvb_fe_sec_voltage(p, 1, 0), true)),
                Some(Voltage::_15v) => Some((dvbv5_sys::dvb_fe_sec_voltage(p, 1, 1), true)),
                _ if clear_unset_lnb => Some((dvbv5_sys::dvb_fe_sec_voltage(p, 0, 0), false)),
                _ => None,
            };
            match lnb_result {
                Some((0, true)) => info!("LNB power enabled."),
                Some((0, false)) => info!("LNB power disabled."),
                Some((rc, enabled)) => warn!(
                    "Failed to {} LNB power: {}",
                    if enabled { "enable" } else { "disable" },
                    Error::from_raw_os_error(-rc)
                ),
                None => {}
            }

            check_dvb_result("tune frontend", dvbv5_sys::dvb_fe_set_parms(p))?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(7);
            loop {
                check_dvb_result("read frontend statistics", dvbv5_sys::dvb_fe_get_stats(p))?;
                // Status is a bitmask, not necessarily a named C enum value.
                let mut status = 0u32;
                check_dvb_result(
                    "read frontend lock",
                    dvbv5_sys::dvb_fe_retrieve_stats(p, DTV_STATUS as c_uint, &mut status),
                )?;
                if status & FE_HAS_LOCK as u32 != 0 {
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    return Err(Error::new(
                        std::io::ErrorKind::TimedOut,
                        "No frontend lock after 7 seconds",
                    ));
                }
                info!("Checking for the frontend lock...");
                std::thread::sleep(WAIT_DUR);
            }
        }
        Ok(())
    }

    fn start_stream(&self) -> Result<ActiveStream, Error> {
        let frontend_id = FrontendId {
            adapter_number: self.id.0,
            frontend_number: self.id.1,
        };
        let demux = DmxFd::new(&frontend_id).map_err(|_| {
            Error::new(
                std::io::ErrorKind::Other,
                "Failed to open the DVB demux device",
            )
        })?;

        unsafe {
            check_dvb_result(
                "start DVB demux",
                dvbv5_sys::dvb_set_pesfilter(
                    demux.as_raw_fd(),
                    0x2000,
                    dmx_ts_pes::DMX_PES_OTHER,
                    dmx_output::DMX_OUT_TS_TAP,
                    1024 * 1024,
                ),
            )?;
        }

        // Wrap the DVR device file in ThreadedReader so that a dedicated
        // background thread continuously drains the kernel's DVB buffer,
        // preventing data drops when the downstream decoder pipeline blocks
        // (e.g., during B-CAS card ECM processing on high-bitrate CS channels).
        let dvr_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(format!("/dev/dvb/adapter{}/dvr{}", self.id.0, self.id.1))?;
        let reader = ThreadedReader::with_defaults(dvr_file)?;
        Ok(ActiveStream {
            stream: BufReader::with_capacity(self.buf_sz, reader),
            demux,
        })
    }

    pub fn tune(self, ch: Channel, lnb: Option<Voltage>) -> Result<Tuner, Error> {
        self.configure_frontend(&ch, lnb.as_ref(), false)?;
        let active = self.start_stream()?;
        Ok(Tuner {
            active: Some(active),
            poisoned: false,
            inner: self,
            state: TunedDvbInternalState::Locked,
        })
    }

    pub fn into_session(self) -> Tuner {
        Tuner {
            active: None,
            poisoned: false,
            inner: self,
            state: TunedDvbInternalState::Stopped,
        }
    }
}

struct ActiveStream {
    // Keep this field before `demux`: the reader must be stopped before
    // closing the demux, which issues DMX_STOP and can strand a DVR read.
    stream: BufReader<ThreadedReader>,
    demux: DmxFd,
}

pub struct Tuner {
    // `active` must be dropped before `inner`, so the DVR reader and demux
    // are gone before the retained frontend is closed.
    active: Option<ActiveStream>,
    poisoned: bool,
    state: TunedDvbInternalState,
    inner: UnTunedTuner,
}

pub enum TunedDvbInternalState {
    Stopped,
    Locked,
    NitScan,
}

impl Tuner {
    fn stop_stream(&mut self) -> Result<(), Error> {
        self.state = TunedDvbInternalState::Stopped;
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let ActiveStream { stream, demux } = active;
        let shutdown_result = stream.into_inner().shutdown();

        // Close the demux only after the DVR worker has released its source.
        drop(demux);

        if let Err(error) = shutdown_result {
            self.poisoned = true;
            return Err(Error::new(
                error.kind(),
                format!("Failed to stop the previous DVB stream: {error}"),
            ));
        }
        Ok(())
    }

    /// Retune while keeping the DVB frontend open.
    ///
    /// The previous DVR reader and demux are discarded so buffered bytes from
    /// one channel cannot cross into the next channel.  A reader shutdown
    /// timeout poisons the tuner; starting another stream would allow the old
    /// worker to consume data from the new channel.
    pub fn retune(&mut self, ch: Channel, lnb: Option<Voltage>) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::new(
                std::io::ErrorKind::BrokenPipe,
                "The DVB tuner cannot be reused after a reader shutdown failure",
            ));
        }

        self.stop_stream()?;
        self.inner.configure_frontend(&ch, lnb.as_ref(), true)?;
        self.active = Some(self.inner.start_stream()?);
        self.state = TunedDvbInternalState::Locked;
        Ok(())
    }

    pub fn signal_quality(&self) -> f64 {
        let p = self.inner.frontend.get_c_ptr();
        unsafe {
            let mut stat = 0u32;
            dvbv5_sys::dvb_fe_get_stats(p);
            dvbv5_sys::dvb_fe_retrieve_stats(p, DTV_STAT_CNR as c_uint, &mut stat as *mut _);
            stat as f64 / 655.35
        }
    }
}

impl AsyncRead for Tuner {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut().active.as_mut() {
            Some(active) => Pin::new(&mut active.stream).poll_read(cx, buf),
            None => Poll::Ready(Err(Error::new(
                std::io::ErrorKind::NotConnected,
                "The DVB tuner is not streaming",
            ))),
        }
    }
}

impl AsyncBufRead for Tuner {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        match self.get_mut().active.as_mut() {
            Some(active) => Pin::new(&mut active.stream).poll_fill_buf(cx),
            None => Poll::Ready(Err(Error::new(
                std::io::ErrorKind::NotConnected,
                "The DVB tuner is not streaming",
            ))),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        if let Some(active) = self.get_mut().active.as_mut() {
            Pin::new(&mut active.stream).consume(amt);
        }
    }
}

fn check_dvb_result(operation: &str, result: i32) -> Result<(), Error> {
    if result < 0 {
        Err(Error::new(
            std::io::ErrorKind::Other,
            format!("Failed to {operation} (libdvbv5 returned {result})"),
        ))
    } else {
        Ok(())
    }
}
