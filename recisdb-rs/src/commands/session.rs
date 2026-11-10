use std::fs::File;
use std::future::Future;
use std::io::{self, BufRead, Read, Write};
use std::os::fd::FromRawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::task::{Context, Poll};

use b25_sys::{DecoderOptions, StreamDecoder};
use futures_util::task::AtomicWaker;
use futures_util::AsyncBufRead;
use log::warn;

use crate::channels::{Channel, ChannelType, StreamFormat};
use crate::tuner::{Tuner, UnTunedTuner, Voltage};

const OUTPUT_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) struct SessionConfig {
    pub device: String,
    pub input_buffer_size: usize,
    pub lnb: Option<Voltage>,
    pub raw: bool,
    pub continue_on_decoder_error: bool,
    pub enable_working_key: bool,
    pub simd: bool,
    pub strip: bool,
}

enum ControlCommand {
    Tune { id: String, channel: String },
    Start { id: String },
    Invalid { id: String, message: String },
    Shutdown,
}

pub(crate) struct DvbSession {
    tuner: Tuner,
    lnb: Option<Voltage>,
    raw: bool,
    decoder_options: DecoderOptions,
    decoder: Option<StreamDecoder>,
    decoder_recovery_attempts: usize,
    continue_on_decoder_error: bool,
    control_rx: Receiver<ControlCommand>,
    control_waker: Arc<AtomicWaker>,
    control_output: File,
    output: File,
    prepared_id: Option<String>,
    started: bool,
    abort: Arc<AtomicBool>,
    bytes_read: u64,
}

impl DvbSession {
    pub(crate) fn new(config: SessionConfig) -> io::Result<Self> {
        let tuner =
            UnTunedTuner::new(config.device, config.input_buffer_size)?.into_dvb_session()?;
        let control_output = duplicate_fd(3)?;
        let output = duplicate_fd(1)?;
        let control_waker = Arc::new(AtomicWaker::new());
        let control_rx = spawn_control_reader(Arc::clone(&control_waker));

        let abort = Arc::new(AtomicBool::new(false));
        let signal_abort = Arc::downgrade(&abort);
        let signal_waker = Arc::downgrade(&control_waker);
        ctrlc::set_handler(move || {
            if let Some(abort) = signal_abort.upgrade() {
                abort.store(true, Ordering::Relaxed);
            }
            if let Some(waker) = signal_waker.upgrade() {
                waker.wake();
            }
        })
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to install the signal handler: {error}"),
            )
        })?;

        Ok(Self {
            tuner,
            lnb: config.lnb,
            raw: config.raw,
            decoder_options: DecoderOptions {
                enable_working_key: config.enable_working_key,
                simd: config.simd,
                strip: config.strip,
                ..DecoderOptions::default()
            },
            decoder: None,
            decoder_recovery_attempts: 0,
            continue_on_decoder_error: config.continue_on_decoder_error,
            control_rx,
            control_waker,
            control_output,
            output,
            prepared_id: None,
            started: false,
            abort,
            bytes_read: 0,
        })
    }

    fn write_response(&mut self, kind: &str, id: &str, message: Option<&str>) -> io::Result<()> {
        write!(self.control_output, "{kind}\t{}", sanitize_field(id))?;
        if let Some(message) = message {
            write!(self.control_output, "\t{}", sanitize_field(message))?;
        }
        writeln!(self.control_output)?;
        self.control_output.flush()
    }

    fn prepare_decoder(&mut self) -> io::Result<()> {
        self.decoder_recovery_attempts = 0;
        if self.raw {
            self.decoder = None;
            return Ok(());
        }

        match self.decoder.as_mut() {
            Some(decoder) => decoder.reset(),
            None => {
                self.decoder = Some(StreamDecoder::new(DecoderOptions {
                    enable_working_key: self.decoder_options.enable_working_key,
                    round: self.decoder_options.round,
                    strip: self.decoder_options.strip,
                    emm: self.decoder_options.emm,
                    simd: self.decoder_options.simd,
                })?);
                Ok(())
            }
        }
    }

    fn handle_tune(&mut self, id: String, channel_name: String) -> io::Result<()> {
        // The existing recisdb wrapper treats a bare numeric Mirakurun channel
        // as terrestrial (for example `13` -> `T13`).  Preserve that contract
        // while leaving explicit T/BS/CS names untouched.
        let channel_name = if channel_name.parse::<u8>().is_ok() {
            format!("T{channel_name}")
        } else {
            channel_name
        };
        let channel = Channel::new(channel_name, None);
        if matches!(channel.ch_type, ChannelType::Undefined) {
            return self.write_response("ERROR", &id, Some("Invalid channel"));
        }
        if channel.stream_format() != StreamFormat::Ts {
            return self.write_response(
                "ERROR",
                &id,
                Some("Persistent sessions do not support ISDB-S3/B61 channels"),
            );
        }
        if !matches!(
            channel.ch_type,
            ChannelType::Terrestrial(..)
                | ChannelType::Catv(..)
                | ChannelType::BS(..)
                | ChannelType::CS(..)
        ) {
            return self.write_response(
                "ERROR",
                &id,
                Some("Persistent sessions support only DVB ISDB-T/S channels"),
            );
        }

        self.started = false;
        self.prepared_id = None;
        // Publish any bytes already accepted by stdout before READY.  Decoder
        // and tuner-side pending buffers are discarded below.
        self.output.flush()?;
        if let Err(error) = self.tuner.retune(channel, self.lnb.clone()) {
            return self.write_response("ERROR", &id, Some(&error.to_string()));
        }
        if let Err(error) = self.prepare_decoder() {
            return self.write_response("ERROR", &id, Some(&error.to_string()));
        }

        self.prepared_id = Some(id.clone());
        self.write_response("READY", &id, None)
    }

    fn handle_start(&mut self, id: String) -> io::Result<()> {
        if self.prepared_id.as_deref() != Some(id.as_str()) {
            return self.write_response(
                "ERROR",
                &id,
                Some("No prepared channel exists for this request"),
            );
        }

        self.started = true;
        Ok(())
    }

    fn handle_command(&mut self, command: ControlCommand) -> io::Result<bool> {
        match command {
            ControlCommand::Tune { id, channel } => self.handle_tune(id, channel)?,
            ControlCommand::Start { id } => self.handle_start(id)?,
            ControlCommand::Invalid { id, message } => {
                self.write_response("ERROR", &id, Some(&message))?
            }
            ControlCommand::Shutdown => return Ok(false),
        }
        Ok(true)
    }

    fn recover_decoder(&mut self, error: io::Error) -> io::Result<()> {
        if !self.continue_on_decoder_error || self.decoder_recovery_attempts >= 3 {
            return Err(error);
        }
        self.decoder_recovery_attempts += 1;
        warn!(
            "Decoder failed ({error}); resetting ({}/3). Buffered packets may be lost.",
            self.decoder_recovery_attempts
        );
        self.decoder
            .as_mut()
            .expect("decoder recovery requires a decoder")
            .reset()
    }

    fn poll_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let buffer = match Pin::new(&mut self.tuner).poll_fill_buf(cx) {
            Poll::Ready(Ok(buffer)) => buffer,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        };
        if buffer.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "DVB tuner stream ended",
            )));
        }

        if self.raw {
            let consumed = buffer.len();
            self.output.write_all(buffer)?;
            Pin::new(&mut self.tuner).consume(consumed);
            self.bytes_read += consumed as u64;
            return Poll::Ready(Ok(()));
        }

        let consumed = match self
            .decoder
            .as_mut()
            .expect("a started decoded session must have a decoder")
            .write(buffer)
        {
            Ok(0) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "B25 decoder accepted no input",
                )))
            }
            Ok(consumed) => consumed,
            Err(error) => {
                // libaribb25 may consume part of a buffer before reporting an
                // error.  Discard the whole buffer rather than replaying it.
                let discarded = buffer.len();
                Pin::new(&mut self.tuner).consume(discarded);
                self.bytes_read += discarded as u64;
                self.recover_decoder(error)?;
                return Poll::Ready(Ok(()));
            }
        };
        Pin::new(&mut self.tuner).consume(consumed);
        self.bytes_read += consumed as u64;

        let mut output_buffer = [0u8; OUTPUT_BUFFER_SIZE];
        loop {
            let count = match self
                .decoder
                .as_mut()
                .expect("a started decoded session must have a decoder")
                .read(&mut output_buffer)
            {
                Ok(count) => count,
                Err(error) => {
                    self.recover_decoder(error)?;
                    return Poll::Ready(Ok(()));
                }
            };
            if count == 0 {
                break;
            }
            self.output.write_all(&output_buffer[..count])?;
        }
        Poll::Ready(Ok(()))
    }
}

impl Future for DvbSession {
    type Output = io::Result<u64>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.control_waker.register(cx.waker());

        if self.abort.load(Ordering::Relaxed) {
            self.output.flush()?;
            return Poll::Ready(Ok(self.bytes_read));
        }

        loop {
            match self.control_rx.try_recv() {
                Ok(command) => {
                    if !self.handle_command(command)? {
                        self.output.flush()?;
                        return Poll::Ready(Ok(self.bytes_read));
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.output.flush()?;
                    return Poll::Ready(Ok(self.bytes_read));
                }
            }
        }

        if self.started {
            match self.poll_stream(cx) {
                Poll::Ready(Ok(())) => {
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

fn duplicate_fd(source: libc::c_int) -> io::Result<File> {
    let fd = unsafe { libc::dup(source) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn spawn_control_reader(waker: Arc<AtomicWaker>) -> Receiver<ControlCommand> {
    let (sender, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("session-control".to_string())
        .spawn(move || {
            for line in io::stdin().lock().lines() {
                let command = match line {
                    Ok(line) => parse_control_line(&line),
                    Err(error) => ControlCommand::Invalid {
                        id: "0".to_string(),
                        message: format!("Failed to read control input: {error}"),
                    },
                };
                if sender.send(command).is_err() {
                    return;
                }
                waker.wake();
            }
            let _ = sender.send(ControlCommand::Shutdown);
            waker.wake();
        })
        .expect("Failed to spawn the session control thread");
    receiver
}

fn parse_control_line(line: &str) -> ControlCommand {
    let fields: Vec<&str> = line.split('\t').collect();
    match fields.as_slice() {
        ["TUNE", id, channel] if !id.is_empty() && !channel.is_empty() => ControlCommand::Tune {
            id: (*id).to_string(),
            channel: (*channel).to_string(),
        },
        ["START", id] if !id.is_empty() => ControlCommand::Start {
            id: (*id).to_string(),
        },
        _ => ControlCommand::Invalid {
            id: fields.get(1).copied().unwrap_or("0").to_string(),
            message: "Expected TUNE\\t<id>\\t<channel> or START\\t<id>".to_string(),
        },
    }
}

fn sanitize_field(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\r' | '\n' | '\t' => ' ',
            character => character,
        })
        .collect()
}
