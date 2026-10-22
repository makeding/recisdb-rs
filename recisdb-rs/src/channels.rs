use crate::channels::representation::TsFilter::{AbsTsId, AsIs, RelTsNum};
pub(crate) use crate::channels::representation::{ChannelSpace, ChannelType};
use log::{error, warn};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamFormat {
    Ts,
    Tlv,
}

pub mod output {
    use crate::channels::representation::{ChannelType, TsFilter};
    use crate::channels::StreamFormat;
    use std::io::{self, ErrorKind};

    #[repr(C)]
    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct IoctlFreq {
        pub ch: i32,
        pub slot: i32,
    }

    impl TryFrom<ChannelType> for IoctlFreq {
        type Error = io::Error;

        fn try_from(value: ChannelType) -> Result<Self, Self::Error> {
            const OFFSET_HZ: i32 = 0;
            if value.stream_format() == StreamFormat::Tlv {
                return s3_ioctl_error();
            }

            let ioctl_channel = match &value {
                ChannelType::Terrestrial(ch_num, ..) if (13..=62).contains(ch_num) => ch_num + 50,
                ChannelType::Catv(ch_num, ..) if (23..=63).contains(ch_num) => ch_num - 1,
                ChannelType::Catv(ch_num, ..) if (13..=22).contains(ch_num) => ch_num - 10,
                ChannelType::CS(ch_num, ..) if (2..=24).contains(ch_num) && (ch_num % 2 == 0) => {
                    ch_num / 2 + 11
                }
                ChannelType::BS(ch_num, _) if (1..=23).contains(ch_num) && (ch_num % 2 == 1) => {
                    ch_num / 2
                }
                ChannelType::BS(..) | ChannelType::CS(..) => return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "ISDB-S3 channels require a DVB frontend; they are unsupported by the character-device ioctl",
                )),
                _ => return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    format!("{value:?} is not supported by the character-device ioctl"),
                )),
            };
            let slot = match value {
                ChannelType::CS(_, TsFilter::AbsTsId(stream_id)) => i32::try_from(stream_id)
                    .map_err(|_| {
                        io::Error::new(ErrorKind::InvalidInput, "TSID exceeds ioctl range")
                    })?,
                ChannelType::CS(..) => 0,
                ChannelType::BS(_, TsFilter::AsIs) => -1,
                ChannelType::BS(_, TsFilter::AbsTsId(stream_id)) => i32::try_from(stream_id)
                    .map_err(|_| {
                        io::Error::new(ErrorKind::InvalidInput, "TSID exceeds ioctl range")
                    })?,
                ChannelType::BS(_, TsFilter::RelTsNum(num)) => num,

                _ => OFFSET_HZ,
            };

            Ok(Self {
                ch: ioctl_channel as i32,
                slot,
            })
        }
    }

    fn s3_ioctl_error() -> io::Result<IoctlFreq> {
        Err(io::Error::new(
            ErrorKind::InvalidInput,
            "ISDB-S3 channels require a DVB frontend; they are unsupported by the character-device ioctl",
        ))
    }

    #[derive(Debug)]
    pub struct DvbFreq {
        pub freq_hz: u32,
        pub stream_id: Option<u32>,
    }

    impl TryFrom<ChannelType> for DvbFreq {
        type Error = io::Error;

        fn try_from(value: ChannelType) -> Result<Self, Self::Error> {
            // Satellite frontends expect this field in kHz despite its historic
            // `freq_hz` name. Terrestrial and CATV values are in Hz.
            let freq_hz: u32 = match &value {
                ChannelType::Terrestrial(ch_num, ..) => {
                    if !(13..=62).contains(ch_num) {
                        return invalid_channel(&value);
                    }
                    let ch_num = *ch_num as u32;
                    473142857 + (ch_num - 13) * 6000000
                }
                ChannelType::Catv(ch_num, ..) => {
                    let ch_num = *ch_num as u32;
                    if (13..=22).contains(&ch_num) {
                        111142857 + (ch_num - 13) * 6000000
                    } else if (23..=63).contains(&ch_num) {
                        225142857 + (ch_num - 23) * 6000000
                    } else {
                        return invalid_channel(&value);
                    }
                }
                ChannelType::BS(ch, ..) if (1..=24).contains(ch) => {
                    let ch = *ch as u32;
                    if ch % 2 == 0 {
                        2241660 + 38360 * (ch / 2 - 1)
                    } else {
                        1049480 + 38360 * (ch / 2)
                    }
                }
                ChannelType::CS(ch, ..) if (1..=25).contains(ch) => {
                    let ch = *ch as u32;
                    if ch % 2 == 0 {
                        1613000 + 40000 * (ch / 2 - 1)
                    } else if ch == 25 {
                        2726000
                    } else {
                        2766000 + 40000 * (ch / 2)
                    }
                }
                _ => return invalid_channel(&value),
            };

            let stream_id = match value {
                ChannelType::Terrestrial(_, TsFilter::AsIs)
                | ChannelType::Catv(_, TsFilter::AsIs) => None,
                ChannelType::BS(_, TsFilter::AsIs) | ChannelType::CS(_, TsFilter::AsIs) => None,
                ChannelType::BS(_, TsFilter::AbsTsId(id))
                | ChannelType::CS(_, TsFilter::AbsTsId(id)) => Some(id),
                ChannelType::BS(ch, TsFilter::RelTsNum(slot)) => {
                    Some(relative_stream_id(ch, slot).ok_or_else(|| {
                        io::Error::new(
                            ErrorKind::InvalidInput,
                            format!("BS{ch}_{slot} has no known stream ID; specify --tsid"),
                        )
                    })?)
                }
                ChannelType::CS(_, TsFilter::RelTsNum(_)) => {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        "relative stream IDs are only defined for BS channels",
                    ))
                }
                _ => return invalid_channel(&value),
            };

            Ok(Self { freq_hz, stream_id })
        }
    }

    fn invalid_channel(value: &ChannelType) -> io::Result<DvbFreq> {
        Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid or unsupported channel: {value:?}"),
        ))
    }

    fn relative_stream_id(ch: u8, slot: i32) -> Option<u32> {
        const STREAM_IDS: &[((u8, i32), u32)] = &[
            ((1, 0), 16400),
            ((1, 1), 16401),
            ((1, 2), 16402),
            ((3, 0), 16432),
            ((3, 1), 17969),
            ((3, 2), 17970),
            ((5, 0), 17488),
            ((5, 1), 17489),
            ((9, 0), 16528),
            ((9, 1), 16530),
            ((13, 0), 16592),
            ((13, 1), 16593),
            ((13, 2), 18130),
            ((15, 0), 16625),
            ((15, 1), 16626),
            ((15, 2), 18675),
            ((19, 0), 18224),
            ((19, 1), 18225),
            ((19, 2), 18226),
            ((19, 3), 18227),
            ((21, 0), 18256),
            ((21, 1), 18257),
            ((21, 2), 18258),
            ((23, 0), 18288),
            ((23, 1), 18801),
            ((23, 2), 18803),
            ((7, 0), 0xb070),
            ((7, 1), 0xb071),
            ((7, 2), 0xb072),
            ((11, 0), 0xb0b0),
            ((11, 1), 0xb0b1),
            ((11, 2), 0xb0b2),
            ((14, 0), 0xb0e0),
            ((17, 0), 0xb110),
            ((17, 1), 0xb111),
            ((17, 2), 0xb112),
        ];
        STREAM_IDS
            .iter()
            .find_map(|&((mapped_ch, mapped_slot), id)| {
                (mapped_ch == ch && mapped_slot == slot).then_some(id)
            })
    }
}

pub mod representation {
    use std::fmt::Display;

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct ChannelSpace {
        pub space: u32,
        pub ch: u32,
        pub space_description: Option<String>,
        pub ch_description: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum TsFilter {
        RelTsNum(i32),
        AbsTsId(u32),
        AsIs,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum ChannelType {
        Terrestrial(u8, TsFilter),
        Catv(u8, TsFilter),
        BS(u8, TsFilter),
        CS(u8, TsFilter),
        BonCh(u8),
        BonChSpace(ChannelSpace),
        Undefined,
    }

    impl Display for ChannelType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            type T = ChannelType;
            match self {
                T::Terrestrial(ch, TsFilter::AsIs) => write!(f, "Terrestrial: {ch}"),
                T::Catv(ch, TsFilter::AsIs) => write!(f, "CATV: {ch}"),
                T::Terrestrial(ch, tsid) => write!(f, "Terrestrial: {} (TS Filter={:?})", ch, tsid),
                T::Catv(ch, tsid) => write!(f, "CATV: {} (TS Filter={:?})", ch, tsid),
                T::BS(ch, tsid) => write!(f, "BS: {}, {:?}", ch, tsid),
                T::CS(ch, tsid) => write!(f, "CS: {}, {:?}", ch, tsid),
                T::BonCh(ch) => write!(f, "BonDriver: Ch={ch}"),
                T::BonChSpace(ChannelSpace { ch, space, .. }) => {
                    write!(f, "BonDriver: Ch={ch}, Space={space}")
                }
                _ => write!(f, "Undefined"),
            }
        }
    }
}

pub struct Channel {
    pub ch_type: ChannelType,
    raw_string: String,
}

impl Channel {
    #[allow(unused)]
    pub fn get_raw_ch_name(&self) -> &str {
        self.raw_string.as_str()
    }

    pub fn stream_format(&self) -> StreamFormat {
        self.ch_type.stream_format()
    }

    pub fn new(ch_str: impl Into<String>, override_stream_id: Option<u32>) -> Self {
        let raw_string = ch_str.into();

        let ch_type = if let Ok(val) = raw_string.parse::<u8>() {
            ChannelType::BonCh(val)
        } else {
            parse_channel_type(&raw_string, override_stream_id)
        };

        let assert_range = |from, to, ch: &u8| {
            if !(from..=to).contains(ch) {
                error!("Channel value {ch} out of range. It must be from {from} to {to}.")
            }
            (from..=to).contains(ch)
        };
        let consistent = match &ch_type {
            ChannelType::Terrestrial(ch, _) => assert_range(13, 62, ch),
            ChannelType::Catv(ch, _) => assert_range(13, 63, ch),
            ChannelType::BS(ch, id) => {
                if matches!(id, RelTsNum(num) if *num < 0) {
                    warn!("The relative TS number cannot be negative.");
                    false
                } else {
                    assert_range(1, 24, ch)
                }
            }
            ChannelType::CS(ch, _) => assert_range(1, 25, ch),
            ChannelType::BonCh(_) | ChannelType::BonChSpace(_) => true,
            _ => false,
        };

        if consistent {
            Self {
                ch_type,
                raw_string,
            }
        } else {
            Self {
                ch_type: ChannelType::Undefined,
                raw_string,
            }
        }
    }
}

impl ChannelType {
    pub fn stream_format(&self) -> StreamFormat {
        match self {
            ChannelType::BS(_, AbsTsId(id)) | ChannelType::CS(_, AbsTsId(id))
                if (0xb000..=0xbfff).contains(id) || (0xc000..=0xcfff).contains(id) =>
            {
                StreamFormat::Tlv
            }
            ChannelType::BS(7 | 11 | 17, _) => StreamFormat::Tlv,
            ChannelType::BS(ch, _) if ch % 2 == 0 => StreamFormat::Tlv,
            ChannelType::CS(ch, _) if ch % 2 != 0 => StreamFormat::Tlv,
            _ => StreamFormat::Ts,
        }
    }
}

fn parse_channel_type(raw: &str, override_stream_id: Option<u32>) -> ChannelType {
    if let Some(suffix) = raw.strip_prefix("BS") {
        let Some((channel, slot)) = parse_channel_and_slot(suffix) else {
            return ChannelType::Undefined;
        };
        return match (slot, override_stream_id) {
            (_, Some(id)) => ChannelType::BS(channel, AbsTsId(id)),
            (Some(slot), None) => ChannelType::BS(channel, RelTsNum(slot)),
            (None, None) => ChannelType::BS(channel, AsIs),
        };
    }
    if let Some(suffix) = raw.strip_prefix("CS") {
        let Some((channel, None)) = parse_channel_and_slot(suffix) else {
            return ChannelType::Undefined;
        };
        return match override_stream_id {
            Some(id) => ChannelType::CS(channel, AbsTsId(id)),
            None => ChannelType::CS(channel, AsIs),
        };
    }
    if let Some(suffix) = raw.strip_prefix('C') {
        return if override_stream_id.is_none() {
            suffix
                .parse::<u8>()
                .map_or(ChannelType::Undefined, |channel| {
                    ChannelType::Catv(channel, AsIs)
                })
        } else {
            ChannelType::Undefined
        };
    }
    if let Some(suffix) = raw.strip_prefix('T') {
        return if override_stream_id.is_none() {
            suffix
                .parse::<u8>()
                .map_or(ChannelType::Undefined, |channel| {
                    ChannelType::Terrestrial(channel, AsIs)
                })
        } else {
            ChannelType::Undefined
        };
    }
    if let Some((space, channel)) = parse_pair(raw) {
        ChannelType::BonChSpace(ChannelSpace {
            space,
            ch: channel,
            space_description: None,
            ch_description: None,
        })
    } else {
        ChannelType::Undefined
    }
}

fn parse_channel_and_slot(input: &str) -> Option<(u8, Option<i32>)> {
    match parse_pair(input) {
        Some((channel, slot)) => {
            Some((u8::try_from(channel).ok()?, Some(i32::try_from(slot).ok()?)))
        }
        None => Some((input.parse::<u8>().ok()?, None)),
    }
}

fn parse_pair(input: &str) -> Option<(u32, u32)> {
    let separator = input.find(|ch| ch == '-' || ch == '_')?;
    let (first, rest) = input.split_at(separator);
    let second = &rest[1..];
    if first.is_empty() || second.is_empty() || second.chars().any(|ch| ch == '-' || ch == '_') {
        return None;
    }
    Some((first.parse().ok()?, second.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::output::*;
    use super::representation::*;
    use super::*;

    #[test]
    fn test_terrestrial_ch_num() {
        let ch_str = "T12";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "T13";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Terrestrial(13, TsFilter::AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "T52";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Terrestrial(52, TsFilter::AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "T62";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Terrestrial(62, TsFilter::AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "T63";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "T64";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());
    }

    #[test]
    fn test_catv_ch_num() {
        let ch_str = "C12";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "C13";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Catv(13, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "C23";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Catv(23, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "C63";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Catv(63, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "C64";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());
    }

    #[test]
    fn test_bs_ch_num() {
        let ch_str = "BS0_2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS1_2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(1, RelTsNum(2)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS03_0";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(3, RelTsNum(0)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS4_2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(4, RelTsNum(2)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS06_0";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(6, RelTsNum(0)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS07_2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(7, RelTsNum(2)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS13_3";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(13, RelTsNum(3)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS17_1";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(17, RelTsNum(1)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS19_9";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::BS(19, RelTsNum(9)));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "BS25_3";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());
    }

    #[test]
    fn test_cs_ch_num() {
        let ch_str = "CS0";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS01";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(1, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(2, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS03";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(3, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS04";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(4, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS24";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(24, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS25";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::CS(25, AsIs));
        assert_eq!(ch.raw_string, ch_str.to_string());

        let ch_str = "CS26";
        let ch = Channel::new(ch_str, None);
        assert_eq!(ch.ch_type, ChannelType::Undefined);
        assert_eq!(ch.raw_string, ch_str.to_string());
    }

    #[test]
    fn test_bon_chspace_from_str() {
        let channel = Channel::new("8", None);
        assert_eq!(channel.ch_type, ChannelType::BonCh(8));

        let ch_str = "1-2";
        let ch = Channel::new(ch_str, None);
        assert_eq!(
            ch.ch_type,
            ChannelType::BonChSpace(ChannelSpace {
                space: 1,
                ch: 2,
                space_description: None,
                ch_description: None,
            })
        );
        assert_eq!(ch.raw_string, ch_str.to_string());
    }

    #[test]
    fn ch_to_ioctl_freq() {
        let ch_str = "T18";
        let ch = Channel::new(ch_str, None);
        let freq = IoctlFreq::try_from(ch.ch_type).unwrap();
        assert_eq!(freq.ch, 68);
        assert_eq!(freq.slot, 0);
    }

    #[test]
    fn dvb_frequency_preserves_b25_mapping_and_exact_tsid() {
        let b25 = DvbFreq::try_from(Channel::new("BS03_1", None).ch_type).unwrap();
        assert_eq!(b25.freq_hz, 1087840);
        assert_eq!(b25.stream_id, Some(17969));

        let explicit = DvbFreq::try_from(Channel::new("BS03", Some(7)).ch_type).unwrap();
        assert_eq!(explicit.stream_id, Some(7));
    }

    #[test]
    fn s3_streams_have_tlv_format_frequency_and_normalized_aliases() {
        let aliases = ["BS7_0", "BS07-0"];
        for alias in aliases {
            let channel = Channel::new(alias, None);
            assert_eq!(channel.ch_type, ChannelType::BS(7, RelTsNum(0)));
            assert_eq!(channel.stream_format(), StreamFormat::Tlv);
            let freq = DvbFreq::try_from(channel.ch_type).unwrap();
            assert_eq!(freq.freq_hz, 1164560);
            assert_eq!(freq.stream_id, Some(0xb070));
        }

        let eight_k = DvbFreq::try_from(Channel::new("BS14_0", None).ch_type).unwrap();
        assert_eq!(eight_k.freq_hz, 2471820);
        assert_eq!(eight_k.stream_id, Some(0xb0e0));

        let cs = Channel::new("CS25", None);
        assert_eq!(cs.stream_format(), StreamFormat::Tlv);
        assert_eq!(DvbFreq::try_from(cs.ch_type).unwrap().freq_hz, 2726000);
    }

    #[test]
    fn explicit_s3_tsid_selects_tlv_without_rewriting_the_id() {
        let channel = Channel::new("BS03", Some(0xb070));
        assert_eq!(channel.stream_format(), StreamFormat::Tlv);
        assert_eq!(
            DvbFreq::try_from(channel.ch_type).unwrap().stream_id,
            Some(0xb070)
        );
    }

    #[test]
    fn missing_relative_slot_is_an_input_error_not_a_no_stream_filter() {
        let channel = Channel::new("BS19_9", None);
        let error = DvbFreq::try_from(channel.ch_type).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }

    #[test]
    fn character_device_rejects_s3_before_ioctl() {
        let error = IoctlFreq::try_from(Channel::new("BS07_0", None).ch_type).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("ISDB-S3"));
    }

    #[test]
    fn parser_fully_consumes_numeric_suffixes() {
        assert_eq!(
            Channel::new("BS7_0junk", None).ch_type,
            ChannelType::Undefined
        );
        assert_eq!(
            Channel::new("BS256_0", None).ch_type,
            ChannelType::Undefined
        );
        assert_eq!(
            Channel::new("1-2junk", None).ch_type,
            ChannelType::Undefined
        );
    }
}
