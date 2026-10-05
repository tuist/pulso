#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=true
export ERL_FLAGS="+S 4:4 +SDcpu 4"
mix test --exclude bench > .auto/tests.out 2>&1 || { tail -80 .auto/tests.out; exit 1; }
cargo test --manifest-path native/pulso_codec/Cargo.toml > .auto/rust-tests.out 2>&1 || { tail -80 .auto/rust-tests.out; exit 1; }
tail -4 .auto/tests.out
