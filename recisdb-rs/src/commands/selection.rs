use crate::channels::StreamFormat;

/// Validate before opening either the tuner or the output file.
pub(super) fn select_b61(
    format: StreamFormat,
    raw: bool,
    force_b61: bool,
    available: bool,
    b25_options: bool,
    proxy: bool,
) -> Result<bool, &'static str> {
    let selected = !raw && (force_b61 || format == StreamFormat::Tlv);
    if proxy && !selected {
        return Err("--cas-proxy requires B61 decoding");
    }
    if selected && b25_options {
        return Err("--card, --key0/--key1 and --no-simd cannot be used with B61 decoding");
    }
    if selected && !available {
        return Err("This TLV recording requires the b61 feature; rebuild with --features dvb,b61 or use --no-decode for raw recording");
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selects_by_format_and_preserves_raw_mode() {
        assert_eq!(
            select_b61(StreamFormat::Ts, false, false, true, false, false),
            Ok(false)
        );
        assert_eq!(
            select_b61(StreamFormat::Tlv, false, false, true, false, true),
            Ok(true)
        );
        assert_eq!(
            select_b61(StreamFormat::Ts, false, true, true, false, false),
            Ok(true)
        );
        assert_eq!(
            select_b61(StreamFormat::Tlv, true, false, false, false, false),
            Ok(false)
        );
    }
    #[test]
    fn rejects_unavailable_or_incompatible_decoders() {
        assert!(select_b61(StreamFormat::Tlv, false, false, false, false, false).is_err());
        assert!(select_b61(StreamFormat::Tlv, false, false, true, true, false).is_err());
        assert!(select_b61(StreamFormat::Ts, false, false, true, false, true).is_err());
        assert!(select_b61(StreamFormat::Tlv, true, false, true, false, true).is_err());
    }
}
