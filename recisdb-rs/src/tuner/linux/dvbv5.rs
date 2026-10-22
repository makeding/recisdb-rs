use crate::channels::output::DvbFreq;
use crate::channels::{Channel, ChannelType};
use crate::tuner::Voltage;
use dvbv5::{DmxFd, FrontendId, FrontendParametersPtr};
use dvbv5_sys::fe_delivery_system::{SYS_ISDBS, SYS_ISDBT};
use dvbv5_sys::fe_sec_voltage::{SEC_VOLTAGE_13, SEC_VOLTAGE_18};
use dvbv5_sys::fe_status::FE_HAS_LOCK;
use dvbv5_sys::{
    dmx_output, dmx_ts_pes, dvb_set_compat_delivery_system, DTV_BANDWIDTH_HZ, DTV_FREQUENCY,
    DTV_ISDBT_LAYER_ENABLED, DTV_ISDBT_PARTIAL_RECEPTION, DTV_ISDBT_SOUND_BROADCASTING, DTV_STATUS,
    DTV_STAT_CNR, DTV_STREAM_ID, DTV_VOLTAGE, NO_STREAM_ID_FILTER,
};
use futures_util::io::{AllowStdIo, BufReader};
use futures_util::{AsyncBufRead, AsyncRead};
use log::{error, info};
use std::ffi::c_uint;
use std::fs::File;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::threaded_reader::ThreadedReader;

pub struct UnTunedTuner {
    id: (u8, u8),
    frontend: FrontendParametersPtr,
    demux: DmxFd,
    pub buf_sz: usize,
}

impl UnTunedTuner {
    pub fn new(adapter_number: u8, fe_number: u8, buf_sz: usize) -> Result<Self, Error> {
        let (frontend, demux) = {
            let frontend_id = FrontendId {
                adapter_number,
                frontend_number: fe_number,
            };

            let f = match FrontendParametersPtr::new(
                &frontend_id,
                Some(if cfg!(debug_assertions) { 1 } else { 0 }),
                Some(false),
            ) {
                Ok(f) => f,
                Err(_) => {
                    error!("Cannot open the device. (Something went wrong while opening DVB frontend device)");
                    std::process::exit(1);
                }
            };
            let d = match DmxFd::new(&frontend_id) {
                Ok(d) => d,
                Err(_) => {
                    error!("Cannot open the device. (Something went wrong while opening DVB demux device)");
                    std::process::exit(1);
                }
            };

            (f, d)
        };

        Ok(Self {
            id: (adapter_number, fe_number),
            frontend,
            demux,
            buf_sz,
        })
    }

    pub fn tune(self, ch: Channel, lnb: Option<Voltage>) -> Result<Tuner, Error> {
        const WAIT_DUR: std::time::Duration = std::time::Duration::from_secs(1);

        let raw_freq = DvbFreq::try_from(ch.ch_type.clone())?;
        unsafe {
            let p = self.frontend.get_c_ptr();
            let system = match ch.ch_type {
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
            match ch.ch_type {
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
                    match lnb {
                        Some(Voltage::_11v) => store(DTV_VOLTAGE, SEC_VOLTAGE_13 as u32)?,
                        Some(Voltage::_15v) => store(DTV_VOLTAGE, SEC_VOLTAGE_18 as u32)?,
                        _ => {}
                    }
                }
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
            check_dvb_result(
                "start DVB demux",
                dvbv5_sys::dvb_set_pesfilter(
                    self.demux.as_raw_fd(),
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
        let dvr_file = File::open(format!("/dev/dvb/adapter{}/dvr{}", self.id.0, self.id.1))?;
        let reader = ThreadedReader::with_defaults(dvr_file)?;
        Ok(Tuner {
            stream: BufReader::with_capacity(self.buf_sz, AllowStdIo::new(reader)),
            inner: self,
            state: TunedDvbInternalState::Locked,
        })
    }
}

pub struct Tuner {
    // Field order matters for drop safety: `stream` (ThreadedReader) must
    // be dropped BEFORE `inner` (frontend/demux). Rust drops fields in
    // declaration order. If `inner` drops first, dvb_dmx_close() issues
    // DMX_STOP which halts data flow to the DVR ring buffer. The reader
    // thread in ThreadedReader may then block forever in read() on the
    // empty ring buffer, causing join() to deadlock. By dropping `stream`
    // first, the reader thread can exit (shutdown flag + receiver dropped)
    // while the device is still actively supplying data.
    stream: BufReader<AllowStdIo<ThreadedReader>>,
    state: TunedDvbInternalState,
    inner: UnTunedTuner,
}

pub enum TunedDvbInternalState {
    Locked,
    NitScan,
}

impl Tuner {
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
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncBufRead for Tuner {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<&[u8]>> {
        Pin::new(&mut self.get_mut().stream).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(&mut self.get_mut().stream).consume(amt)
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
