//! Safe `Read`/`Write` wrapper for the installed libaribb61 C ABI.
//!
//! The library owns the output returned by `get()`. This wrapper copies it
//! before any later mutating call so ordinary small Rust reads cannot lose it.

use std::cell::Cell;
use std::ffi::CString;
use std::io::{self, ErrorKind, Read, Write};
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use log::warn;

#[repr(C)]
struct AribStdB61Buffer {
    data: *mut u8,
    size: u32,
}

enum AribStdB61 {}

extern "C" {
    fn create_arib_std_b61() -> *mut AribStdB61;
    fn arib_std_b61_release(ctx: *mut AribStdB61);
    fn arib_std_b61_put(ctx: *mut AribStdB61, buf: *const AribStdB61Buffer) -> i32;
    fn arib_std_b61_get(ctx: *mut AribStdB61, buf: *mut AribStdB61Buffer) -> i32;
    fn arib_std_b61_flush(ctx: *mut AribStdB61) -> i32;
    fn arib_std_b61_reset(ctx: *mut AribStdB61) -> i32;
    fn arib_std_b61_set_strip(ctx: *mut AribStdB61, enabled: i32) -> i32;
    fn arib_std_b61_set_emm_proc(ctx: *mut AribStdB61, enabled: i32) -> i32;
    fn arib_std_b61_set_cas_proxy(ctx: *mut AribStdB61, address: *const std::ffi::c_char) -> i32;
}

/// B61 stream-decoder options. The CAS proxy is an endpoint understood by
/// libaribb61; no proxy is used when it is `None`.
#[derive(Debug, Clone, Default)]
pub struct DecoderOptions {
    pub strip: bool,
    pub emm: bool,
    pub cas_proxy: Option<String>,
}

/// Streaming B61 decoder. It is deliberately not `Sync`: libaribb61 requires
/// callers to serialize access to each instance.
pub struct StreamDecoder {
    ctx: NonNull<AribStdB61>,
    pending: Vec<u8>,
    pending_offset: usize,
    last_encrypted_warning: Cell<Option<Instant>>,
}

impl StreamDecoder {
    pub fn new(options: DecoderOptions) -> io::Result<Self> {
        let ctx = NonNull::new(unsafe { create_arib_std_b61() }).ok_or_else(|| {
            io::Error::new(ErrorKind::Other, "libaribb61 could not create a decoder")
        })?;
        let mut decoder = Self {
            ctx,
            pending: Vec::new(),
            pending_offset: 0,
            last_encrypted_warning: Cell::new(None),
        };
        if let Err(error) = decoder.configure(options) {
            unsafe { arib_std_b61_release(decoder.ctx.as_ptr()) };
            std::mem::forget(decoder);
            return Err(error);
        }
        Ok(decoder)
    }

    fn configure(&mut self, options: DecoderOptions) -> io::Result<()> {
        self.result(unsafe { arib_std_b61_set_strip(self.ctx.as_ptr(), options.strip as i32) })?;
        self.result(unsafe { arib_std_b61_set_emm_proc(self.ctx.as_ptr(), options.emm as i32) })?;
        if let Some(proxy) = options.cas_proxy {
            let proxy = CString::new(proxy)
                .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "CAS proxy contains NUL"))?;
            self.result(unsafe { arib_std_b61_set_cas_proxy(self.ctx.as_ptr(), proxy.as_ptr()) })?;
        }
        Ok(())
    }

    /// Discard buffered input/output and restore decoder state while retaining
    /// the selected options, as defined by libaribb61.
    pub fn reset(&mut self) -> io::Result<()> {
        self.pending.clear();
        self.pending_offset = 0;
        self.last_encrypted_warning.set(None);
        self.result(unsafe { arib_std_b61_reset(self.ctx.as_ptr()) })
    }

    fn result(&self, code: i32) -> io::Result<()> {
        match code {
            0 => Ok(()),
            1 => {
                let now = Instant::now();
                if self.last_encrypted_warning.get().map_or(true, |last| {
                    now.duration_since(last) >= Duration::from_secs(30)
                }) {
                    warn!("libaribb61 accepted input but left one or more packets encrypted");
                    self.last_encrypted_warning.set(Some(now));
                }
                Ok(())
            }
            -1 => Err(io::Error::new(
                ErrorKind::InvalidInput,
                "libaribb61: invalid parameter",
            )),
            -2 => Err(io::Error::new(
                ErrorKind::Other,
                "libaribb61: processing failed",
            )),
            -3 => Err(io::Error::new(
                ErrorKind::Other,
                "libaribb61: out of memory",
            )),
            -4 => Err(io::Error::new(
                ErrorKind::Other,
                "libaribb61: invalid decoder state; call reset",
            )),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                format!("libaribb61: unexpected status {code}"),
            )),
        }
    }

    fn refill_pending(&mut self) -> io::Result<()> {
        let mut result = AribStdB61Buffer {
            data: std::ptr::null_mut(),
            size: 0,
        };
        self.result(unsafe { arib_std_b61_get(self.ctx.as_ptr(), &mut result) })?;
        if result.size != 0 {
            if result.data.is_null() {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "libaribb61 returned a null output buffer",
                ));
            }
            let output = unsafe { std::slice::from_raw_parts(result.data, result.size as usize) };
            self.pending.extend_from_slice(output);
        }
        Ok(())
    }

    fn drain_pending(&mut self, out: &mut [u8]) -> usize {
        let available = self.pending.len().saturating_sub(self.pending_offset);
        let count = available.min(out.len());
        out[..count]
            .copy_from_slice(&self.pending[self.pending_offset..self.pending_offset + count]);
        self.pending_offset += count;
        if self.pending_offset == self.pending.len() {
            self.pending.clear();
            self.pending_offset = 0;
        }
        count
    }
}

impl Drop for StreamDecoder {
    fn drop(&mut self) {
        unsafe { arib_std_b61_release(self.ctx.as_ptr()) };
    }
}

impl Write for StreamDecoder {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if input.len() > u32::MAX as usize {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "input exceeds libaribb61 buffer limit",
            ));
        }
        let buffer = AribStdB61Buffer {
            data: input.as_ptr() as *mut u8,
            size: input.len() as u32,
        };
        self.result(unsafe { arib_std_b61_put(self.ctx.as_ptr(), &buffer) })?;
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.result(unsafe { arib_std_b61_flush(self.ctx.as_ptr()) })
    }
}

impl Read for StreamDecoder {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.pending_offset == self.pending.len() {
            self.refill_pending()?;
        }
        Ok(self.drain_pending(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clear_tlv() -> Vec<u8> {
        // A complete clear IPv4 TLV packet requires no card. Fragmentation
        // exercises the C framer, then small reads exercise the copied tail.
        vec![0x7f, 0x01, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef]
    }

    #[test]
    fn fragmented_clear_tlv_survives_small_reads_and_flush() {
        let input = clear_tlv();
        let mut decoder = StreamDecoder::new(DecoderOptions::default()).unwrap();
        decoder.write_all(&input[..3]).unwrap();
        assert_eq!(decoder.read(&mut [0; 1]).unwrap(), 0);
        decoder.write_all(&input[3..]).unwrap();
        decoder.flush().unwrap();
        let mut output = Vec::new();
        let mut small = [0; 2];
        loop {
            let count = decoder.read(&mut small).unwrap();
            if count == 0 {
                break;
            }
            output.extend_from_slice(&small[..count]);
        }
        assert_eq!(output, input);
    }

    #[test]
    fn copied_small_read_remains_ordered_across_another_write() {
        let input = clear_tlv();
        let mut decoder = StreamDecoder::new(DecoderOptions::default()).unwrap();
        decoder.write_all(&input).unwrap();
        let mut first = [0; 3];
        decoder.read_exact(&mut first).unwrap();
        decoder.write_all(&input).unwrap();
        decoder.flush().unwrap();

        let mut output = first.to_vec();
        decoder.read_to_end(&mut output).unwrap();
        assert_eq!(output, [input.clone(), input].concat());
    }
}
