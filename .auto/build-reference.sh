#!/bin/bash
set -euo pipefail
# Frozen control: last kept production codec, including metric labels sharing.
mkdir -p .auto/reference
git archive 8876777 native/pulso_codec | tar -x -C .auto/reference
python3 - <<'PY'
from pathlib import Path
root = Path('.auto/reference/native/pulso_codec')
p = root / 'Cargo.toml'
p.write_text(p.read_text().replace('name = "pulso_codec"', 'name = "pulso_codec_reference"'))
p = root / 'src/lib.rs'
p.write_text(p.read_text().replace('rustler::init!("Elixir.Pulso.Codec.NIF");', 'rustler::init!("Elixir.Pulso.AutoReferenceNIF");'))
PY
CARGO_TARGET_DIR="$PWD/native/pulso_codec/target" cargo build --release --manifest-path .auto/reference/native/pulso_codec/Cargo.toml > .auto/reference-build.out 2>&1 || { tail -80 .auto/reference-build.out; exit 1; }
cp native/pulso_codec/target/release/libpulso_codec_reference.dylib .auto/reference/libpulso_codec_reference.so
