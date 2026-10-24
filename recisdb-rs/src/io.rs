use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use futures_util::io::{AllowStdIo, BufReader};
use futures_util::{AsyncBufRead, AsyncWrite};
use log::{error, info, warn};
use pin_project_lite::pin_project;

use b25_sys::{DecoderOptions as B25DecoderOptions, StreamDecoder as B25StreamDecoder};
#[cfg(feature = "b61")]
use b61_sys::{DecoderOptions as B61DecoderOptions, StreamDecoder as B61StreamDecoder};

pub(crate) enum DecoderConfig {
    B25(B25DecoderOptions),
    #[cfg(feature = "b61")]
    B61(B61DecoderOptions),
}

enum DecoderBackend {
    #[cfg(test)]
    Mock(tests::MockDecoder),
    B25(B25StreamDecoder),
    #[cfg(feature = "b61")]
    B61(B61StreamDecoder),
}

impl std::io::Read for DecoderBackend {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(test)]
            Self::Mock(decoder) => std::io::Read::read(decoder, buf),
            Self::B25(decoder) => std::io::Read::read(decoder, buf),
            #[cfg(feature = "b61")]
            Self::B61(decoder) => std::io::Read::read(decoder, buf),
        }
    }
}

impl std::io::Write for DecoderBackend {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(test)]
            Self::Mock(decoder) => std::io::Write::write(decoder, buf),
            Self::B25(decoder) => std::io::Write::write(decoder, buf),
            #[cfg(feature = "b61")]
            Self::B61(decoder) => std::io::Write::write(decoder, buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(test)]
            Self::Mock(_) => Ok(()),
            Self::B25(decoder) => std::io::Write::flush(decoder),
            #[cfg(feature = "b61")]
            Self::B61(decoder) => std::io::Write::flush(decoder),
        }
    }
}

pin_project! {
    pub(crate) struct AsyncInOutTriple {
        #[pin]
        i: Box<dyn AsyncBufRead + Unpin + 'static>,
        o: AllowStdIo<Box<dyn Write>>,
        dec: RefCell<Option<BufReader<AllowStdIo<DecoderBackend>>>>,
        amt: u64,
        recovery: DecoderRecovery,
        abort: Arc<AtomicBool>,
        progress_tx: std::sync::mpsc::Sender<u64>,
    }
}

impl AsyncInOutTriple {
    const CAP: usize = 1600000;
    pub fn new(
        i: Box<dyn AsyncBufRead + Unpin>,
        o: Box<dyn Write>,
        config: Option<DecoderConfig>,
        continue_on_error: bool,
    ) -> (Self, std::sync::mpsc::Receiver<u64>) {
        let raw = config.and_then(|op| match DecoderBackend::new(op) {
            Ok(raw) => Some(raw),
            Err(e) => {
                error!("Error occurred while initializing the decoder. ({})", e);
                error!("Check the CAS reader or proxy connection.");
                std::process::exit(-1)
            }
        });

        let dec = {
            let buffered_decoder = raw
                .map(AllowStdIo::new)
                .map(|raw| BufReader::with_capacity(Self::CAP, raw));

            RefCell::new(buffered_decoder)
        };

        let o = AllowStdIo::new(o);

        let abort: Arc<AtomicBool> = Default::default();
        let weak = Arc::downgrade(&abort);
        ctrlc::set_handler(move || {
            if let Some(ptr) = weak.upgrade() {
                ptr.store(true, Ordering::Relaxed)
            }
        })
        .expect("Error setting Ctrl-C handler");

        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        (
            Self {
                i,
                o,
                dec,
                amt: 0,
                abort,
                progress_tx,
                recovery: DecoderRecovery {
                    enabled: continue_on_error,
                    attempts: 0,
                },
            },
            progress_rx,
        )
    }
}

impl DecoderBackend {
    fn reset(&mut self) -> io::Result<()> {
        match self {
            #[cfg(test)]
            Self::Mock(decoder) => {
                decoder.pending.clear();
                Ok(())
            }
            Self::B25(decoder) => decoder.reset(),
            #[cfg(feature = "b61")]
            Self::B61(decoder) => decoder.reset(),
        }
    }

    fn new(config: DecoderConfig) -> io::Result<Self> {
        match config {
            DecoderConfig::B25(options) => B25StreamDecoder::new(options).map(Self::B25),
            #[cfg(feature = "b61")]
            DecoderConfig::B61(options) => B61StreamDecoder::new(options).map(Self::B61),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Mutex;

    pub(super) struct MockDecoder {
        pub pending: Vec<u8>,
        failures: usize,
    }
    impl Write for MockDecoder {
        fn write(&mut self, input: &[u8]) -> io::Result<usize> {
            if self.failures > 0 {
                self.failures -= 1;
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "injected decoder failure",
                ));
            }
            self.pending
                .extend(input.iter().map(|b| b.to_ascii_uppercase()));
            Ok(input.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Read for MockDecoder {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            let n = out.len().min(self.pending.len());
            out[..n].copy_from_slice(&self.pending[..n]);
            self.pending.drain(..n);
            Ok(n)
        }
    }
    struct Output(Arc<Mutex<Vec<u8>>>);
    impl Write for Output {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn run(failures: usize, enabled: bool, raw: bool) -> (io::Result<u64>, Vec<u8>) {
        let output = Arc::new(Mutex::new(Vec::new()));
        let (progress_tx, _rx) = std::sync::mpsc::channel();
        let body = AsyncInOutTriple {
            i: Box::new(BufReader::with_capacity(
                1,
                futures_util::io::Cursor::new(b"abcde".to_vec()),
            )),
            o: AllowStdIo::new(Box::new(Output(output.clone()))),
            dec: RefCell::new(if raw {
                None
            } else {
                Some(BufReader::new(AllowStdIo::new(DecoderBackend::Mock(
                    MockDecoder {
                        pending: Vec::new(),
                        failures,
                    },
                ))))
            }),
            amt: 0,
            recovery: DecoderRecovery {
                enabled,
                attempts: 0,
            },
            abort: Arc::new(AtomicBool::new(false)),
            progress_tx,
        };
        let result = futures_executor::block_on(body);
        let bytes = output.lock().unwrap().clone();
        (result, bytes)
    }
    #[test]
    fn transient_failure_resumes_decoding_without_raw_output() {
        let (result, output) = run(1, true, false);
        assert_eq!(result.unwrap(), 4);
        assert_eq!(output, b"BCDE");
    }
    #[test]
    fn persistent_failure_exits_without_raw_output() {
        let (result, output) = run(4, true, false);
        assert!(result.is_err());
        assert!(output.is_empty());
    }
    #[test]
    fn strict_failure_exits_immediately() {
        let (result, output) = run(1, false, false);
        assert!(result.is_err());
        assert!(output.is_empty());
    }
    #[test]
    fn explicit_raw_passthrough_is_unchanged() {
        let (result, output) = run(0, true, true);
        assert_eq!(result.unwrap(), 5);
        assert_eq!(output, b"abcde");
    }
}

struct DecoderRecovery {
    enabled: bool,
    // Lifetime budget: repeated errors must eventually surface as failure.
    attempts: usize,
}

impl DecoderRecovery {
    fn recover(
        &mut self,
        error: io::Error,
        reset: impl FnOnce() -> io::Result<()>,
    ) -> io::Result<()> {
        if !self.enabled || self.attempts >= 3 {
            return Err(error);
        }
        self.attempts += 1;
        warn!("Decoder failed ({error}); resetting ({}/3). Buffered packets may be lost; raw passthrough is disabled.", self.attempts);
        reset()
    }
}

impl Future for AsyncInOutTriple {
    type Output = io::Result<u64>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        let _ = this.progress_tx.send(*this.amt);

        match this.dec.get_mut() {
            Some(ref mut dec) => {
                //    A.         B.
                // In -> Decoder -> Out
                if !this.abort.load(Ordering::Relaxed) {
                    // A(source)
                    let buffer = ready!(this.i.as_mut().poll_fill_buf(cx))?;
                    if buffer.is_empty() {
                        // go to finalization
                        this.abort.store(true, Ordering::Relaxed);
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    } else {
                        // A(sink)
                        match ready!(Pin::new(&mut *dec).poll_write(cx, buffer)) {
                            Ok(0) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                            Ok(i) => {
                                *this.amt += i as u64;
                                this.i.as_mut().consume(i);
                            }
                            Err(e) => {
                                // put() may have consumed part of this buffer before failing.
                                // Never replay it or pass it through as encrypted output.
                                let discarded = buffer.len();
                                this.i.as_mut().consume(discarded);
                                this.recovery
                                    .recover(e, || dec.get_mut().get_mut().reset())?;
                                // Also discard the Rust-side decoded buffer from before reset.
                                let buffered = dec.buffer().len();
                                Pin::new(&mut *dec).consume(buffered);
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                        }
                    }

                    // B(source)
                    let buffer = match ready!(Pin::new(&mut *dec).poll_fill_buf(cx)) {
                        Ok(buffer) => buffer,
                        Err(e) => {
                            this.recovery
                                .recover(e, || dec.get_mut().get_mut().reset())?;
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    };
                    if !buffer.is_empty() {
                        // B(sink)
                        let j = ready!(Pin::new(&mut this.o).poll_write(cx, buffer))?;
                        if j == 0 {
                            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                        }
                        Pin::new(&mut *dec).consume(j);
                    }

                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                // Finalize
                let _ = this.progress_tx.send(u64::MAX);
                info!("Flushing the buffer…");

                // A(sink)
                ready!(Pin::new(&mut *dec).poll_flush(cx))?;
                // B(source)
                loop {
                    match Pin::new(&mut *dec).poll_fill_buf(cx) {
                        Poll::Ready(Ok(buffer)) if buffer.is_empty() => {
                            ready!(Pin::new(&mut this.o).poll_flush(cx))?;
                            return Poll::Ready(Ok(*this.amt));
                        }
                        Poll::Ready(Ok(buffer)) => {
                            // B(sink)
                            let j = ready!(Pin::new(&mut this.o).poll_write(cx, buffer))?;
                            if j == 0 {
                                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                            }
                            Pin::new(&mut *dec).consume(j);
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => continue,
                    }
                }
            }
            _ => {
                // pass through
                let buffer = ready!(this.i.as_mut().poll_fill_buf(cx))?;
                if buffer.is_empty() || this.abort.load(Ordering::Relaxed) {
                    ready!(Pin::new(&mut this.o).poll_flush(cx))?;
                    return Poll::Ready(Ok(*this.amt));
                }

                let i = ready!(Pin::new(&mut this.o).poll_write(cx, buffer))?;
                if i == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                *this.amt += i as u64;
                this.i.as_mut().consume(i);

                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
