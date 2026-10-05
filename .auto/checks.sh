#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=1
mise exec -- mix test --seed 42 > /tmp/pulso-cost-checks.log 2>&1 || { tail -80 /tmp/pulso-cost-checks.log; exit 1; }
tail -4 /tmp/pulso-cost-checks.log
mise exec -- cargo test --release --manifest-path native/pulso_codec/Cargo.toml long_utf8_statistics_are_conservative_and_timestamp_bounds_stay_exact > /tmp/pulso-cost-rust-checks.log 2>&1 || { tail -80 /tmp/pulso-cost-rust-checks.log; exit 1; }
tail -4 /tmp/pulso-cost-rust-checks.log
