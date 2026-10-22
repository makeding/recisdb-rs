use futures_time::time::Duration;
use std::future::Future;
#[cfg(any(target_os = "linux", target_os = "windows"))]
use std::io::Write;

#[cfg(any(
    target_os = "linux",
    target_os = "windows",
    not(feature = "prioritized_card_reader"),
    feature = "b61"
))]
use log::warn;
use log::{error, info};

use b25_sys::DecoderOptions;
#[cfg(feature = "b61")]
use b61_sys::DecoderOptions as B61DecoderOptions;

#[cfg(any(target_os = "linux", target_os = "windows"))]
use crate::channels::{Channel, ChannelType};
use crate::commands::utils::parse_keys;
use crate::context::{Cli, Commands};
use crate::io::{AsyncInOutTriple, DecoderConfig};
#[cfg(any(target_os = "linux", target_os = "windows"))]
use crate::tuner::{Tunable, UnTunedTuner};

#[cfg(any(test, target_os = "linux", target_os = "windows"))]
mod selection;
pub(crate) mod utils;

/// The behavior the user requested are returned.
/// If an error occurred during preparation, the program bails out with expect().
pub(crate) fn process_command(
    args: Cli,
) -> (
    impl Future<Output = std::io::Result<u64>>,
    Option<Duration>,
    Option<(u64, std::sync::mpsc::Receiver<u64>)>,
) {
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    const INPUT_BUF_DEFAULT: usize = 200000;
    #[cfg(any(target_os = "linux", target_os = "windows"))]
    let buf_sz = std::env::var("RECISDB_INPUT_BUF_BYTES")
        .unwrap_or("".to_string())
        .parse()
        .unwrap_or(INPUT_BUF_DEFAULT);

    match args.command {
        #[cfg(any(target_os = "linux", target_os = "windows"))]
        Commands::Checksignal {
            channel,
            device,
            lnb,
        } => {
            // Get channel
            let channel = channel.map(|ch| Channel::new(ch, None)).unwrap();
            if let ChannelType::Undefined = channel.ch_type {
                error!("The specified channel is invalid.");
                std::process::exit(1);
            }
            info!("Tuner: {}", device);
            info!(
                "Channel: {} / {}",
                channel.get_raw_ch_name(),
                channel.ch_type
            );

            // Open tuner and tune to channel
            let tuned = match UnTunedTuner::new(device, 0)
                .map_err(|e| utils::error_handler::handle_opening_error(e.into()))
                .unwrap()
                .tune(channel, lnb)
            {
                Ok(inner) => inner,
                Err(e) => utils::error_handler::handle_tuning_error(e),
            };

            // ctrlc::set_handler(|| std::process::exit(0)).expect("Error setting Ctrl-C handler");

            loop {
                print!("\r{:.2}dB", tuned.signal_quality());
                std::io::stdout().flush().unwrap();
                std::thread::sleep(Duration::from_secs_f64(1.0).into())
            }
        }
        #[cfg(any(target_os = "linux", target_os = "windows"))]
        Commands::Tune {
            device,
            channel,
            card,
            tsid,
            time,
            no_decode: disable_decode,
            lnb,
            key0,
            key1,
            no_simd,
            no_strip,
            b61,
            cas_proxy,
            output,
            exit_on_card_error,
        } => {
            let channel = channel.map(|ch| Channel::new(ch, tsid)).unwrap();
            if let ChannelType::Undefined = channel.ch_type {
                error!("The specified channel is invalid.");
                std::process::exit(1);
            }
            let use_b61 = selection::select_b61(
                channel.stream_format(),
                disable_decode,
                b61,
                cfg!(feature = "b61"),
                card.is_some() || key0.is_some() || key1.is_some() || no_simd,
                cas_proxy.is_some(),
            )
            .unwrap_or_else(|message| {
                error!("{message}");
                std::process::exit(1)
            });
            if let Some(name) = card {
                #[cfg(not(feature = "prioritized_card_reader"))]
                warn!("--card {name} has no effect. Use `prioritized_card_reader` feature flag.");
                #[cfg(feature = "prioritized_card_reader")]
                b25_sys::set_card_reader_name(&name);
            }
            info!("Tuner: {}", device.clone().unwrap());
            info!(
                "Channel: {} / {}",
                channel.get_raw_ch_name(),
                channel.ch_type
            );

            // Recording duration
            let rec_duration = time.map(Duration::from_secs_f64);
            match rec_duration {
                Some(duration) => {
                    info!("Recording duration: {} seconds", duration.as_secs_f64());
                }
                None => {
                    info!("Recording duration: Infinite");
                }
            }

            // in, out, dec
            let input = utils::get_tuner_src(device.unwrap(), channel, lnb, buf_sz)
                .map_err(|e| {
                    error!("Failed to open input source: {}", e);
                    std::process::exit(1);
                })
                .unwrap();
            let output = utils::get_output(output)
                .map_err(|e| {
                    error!("Failed to open output: {}", e.kind());
                    std::process::exit(1);
                })
                .unwrap();
            let dec = if disable_decode {
                info!("Decode: Disabled");
                None
            } else {
                info!("Decode: Enabled");
                if use_b61 {
                    #[cfg(feature = "b61")]
                    {
                        Some(DecoderConfig::B61(B61DecoderOptions {
                            strip: !no_strip,
                            cas_proxy,
                            ..B61DecoderOptions::default()
                        }))
                    }
                    #[cfg(not(feature = "b61"))]
                    {
                        error!("B61 decoding requires building recisdb with the `b61` feature");
                        std::process::exit(1)
                    }
                } else {
                    Some(DecoderConfig::B25(DecoderOptions {
                        enable_working_key: parse_keys(key0, key1),
                        simd: !no_simd,
                        strip: !no_strip,
                        ..DecoderOptions::default()
                    }))
                }
            };

            let (body, _) = AsyncInOutTriple::new(input, output, dec, !exit_on_card_error);
            info!("Recording...");
            (body, rec_duration, None)
        }
        Commands::Decode {
            source,
            card,
            key0,
            key1,
            no_simd,
            no_strip,
            b61,
            cas_proxy,
            output,
        } => {
            #[cfg(not(feature = "b61"))]
            if b61 {
                error!("B61 decoding requires building recisdb with the `b61` feature");
                std::process::exit(1)
            }

            // Card reader
            if let Some(name) = card {
                #[cfg(not(feature = "prioritized_card_reader"))]
                warn!("--card {name} has no effect. Use `prioritized_card_reader` feature flag.");

                #[cfg(feature = "prioritized_card_reader")]
                b25_sys::set_card_reader_name(&name);
            }

            // in, out, dec
            let (input, input_sz) = utils::get_file_src(source)
                .map_err(|e| {
                    error!("Failed to open input source: {}", e);
                    std::process::exit(1);
                })
                .unwrap();
            let output = utils::get_output(output)
                .map_err(|e| {
                    error!("Failed to open output: {}", e.kind());
                    std::process::exit(1);
                })
                .unwrap();
            let dec = if b61 {
                #[cfg(feature = "b61")]
                {
                    if key0.is_some() || key1.is_some() || no_simd {
                        warn!("--key0/--key1 and --no-simd do not apply to B61 decoding");
                    }
                    Some(DecoderConfig::B61(B61DecoderOptions {
                        strip: !no_strip,
                        cas_proxy,
                        ..B61DecoderOptions::default()
                    }))
                }
                #[cfg(not(feature = "b61"))]
                {
                    error!("B61 decoding requires building recisdb with the `b61` feature");
                    std::process::exit(1)
                }
            } else {
                Some(DecoderConfig::B25(DecoderOptions {
                    enable_working_key: parse_keys(key0, key1),
                    simd: !no_simd,
                    strip: !no_strip,
                    ..DecoderOptions::default()
                }))
            };

            let (body, progress) = AsyncInOutTriple::new(input, output, dec, false);
            info!("Decoding...");
            (body, None, input_sz.map(|sz| (sz, progress)))
        }
        #[cfg(windows)]
        Commands::Enumerate { device, space } => {
            // Open tuner
            let untuned = UnTunedTuner::new(device, buf_sz)
                .map_err(|e| utils::error_handler::handle_opening_error(e.into()))
                .unwrap();
            if let Some(spacename_channels) = untuned.enum_channels(space) {
                for item in spacename_channels {
                    println!("{}", item)
                }
                std::process::exit(0)
            } else {
                std::process::exit(1)
            }
        }
    }
}
