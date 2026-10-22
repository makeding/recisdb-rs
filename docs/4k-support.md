# ISDB-S3 support contract

Channel parsing owns physical frequency, resolved stream ID and TS/TLV format.
Linux DVB consumes those parameters; it does not parse channel names or resolve
relative slots. It uses the recdvb driver ABI: SYS_ISDBS, frequency in kHz and
DTV_STREAM_ID. Terrestrial frequencies remain in Hz.

The initial static mapping preserves the previous ISDB-S entries and adds
BS07, BS11, BS17 slots 0–2 and BS14 slot 0 from the local recdvb reference.
These are reference mappings, not a live broadcaster directory. Explicit
--tsid values are preserved exactly; unknown relative slots fail with guidance
to specify --tsid. BS1..24 and CS1..25 are accepted physical ranges.

The recording pipeline chooses B25 for TS and B61 for known TLV channels.
--b61 overrides the decoder for sources whose format cannot be inferred.
--no-decode bypasses decoding. Missing B61 support and incompatible B25-only
options fail before opening input/output. --cas-proxy applies only to B61.
Legacy character-device tuning rejects S3 before changing tuner/LNB settings.
BonDriver remains driver-dependent and can use the explicit decoder override.

The shared reader/output pipeline transports bytes without TS remuxing.
DVB setup errors and lock timeout must be returned; recording must not proceed
after a failed tuning or demux operation.

B61 follows B25's dependency layout: b61-sys/externals/libaribb61 is a pinned
Git submodule. Cargo builds and statically links it on macOS/Windows, and uses
pkg-config on Linux with a bundled fallback. Bundled builds require CMake,
standalone Asio headers and the platform PC/SC development library. No separate
libaribb61 installation or runtime library path is needed for bundled builds.
Initialize dependencies with git submodule update --init --recursive.

## Validation boundary

Pure regression tests cover parsing, frequency/slot resolution, exact explicit
IDs, TS/TLV selection and unsupported legacy tuning. Builds and clear-TLV
decode checks do not establish RF lock, encrypted multi-service correctness,
continuous recording or A/V playback. Those require an S3-capable Linux DVB
device, an authorized CAS source and a real recording.

libaribb61 remains experimental: its initial compressed-IP/AES implementation
can leave packets encrypted when keys are unavailable, and does not yet provide
per-service ECM routing or replay of startup packets awaiting keys.

Local verification on macOS (2026-10-22): default workspace tests passed
(14 recisdb tests); B61-enabled tests passed (14 recisdb + 2 wrapper tests);
the B61-enabled CLI built and preserved the clear TLV packet
7f010004deadbeef through stdin/stdout. cargo fmt and git diff --check passed.
Linux DVB compilation, RF lock and encrypted 4K recording remain unverified.

Bundled-build verification (2026-10-22): Cargo built the submodule's static
library without PKG_CONFIG_PATH or DYLD_LIBRARY_PATH overrides; all 16 tests
passed. The CLI preserved the clear TLV sample without either override, and
otool -L showed no libaribb61 dynamic dependency. Linux/Windows bundled builds
remain unverified locally. Linux/macOS CI now includes the B61 build and tests.
The static-build dependency commit must be pushed before sharing the parent
repository's updated submodule pointer.
