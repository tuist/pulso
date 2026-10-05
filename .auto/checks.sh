#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=1
mise exec -- mix test --seed 42 > /tmp/pulso-cost-checks.log 2>&1 || { tail -80 /tmp/pulso-cost-checks.log; exit 1; }
tail -4 /tmp/pulso-cost-checks.log
mise exec -- mix format --check-formatted > /tmp/pulso-cost-format-elixir.log 2>&1 || { tail -80 /tmp/pulso-cost-format-elixir.log; exit 1; }
mise exec -- mix credo > /tmp/pulso-cost-credo.log 2>&1 || { tail -80 /tmp/pulso-cost-credo.log; exit 1; }
mise exec -- cargo fmt --manifest-path native/pulso_codec/Cargo.toml -- --check > /tmp/pulso-cost-format-rust.log 2>&1 || { tail -80 /tmp/pulso-cost-format-rust.log; exit 1; }
mise exec -- cargo clippy --manifest-path native/pulso_codec/Cargo.toml --all-targets -- -D warnings > /tmp/pulso-cost-clippy.log 2>&1 || { tail -80 /tmp/pulso-cost-clippy.log; exit 1; }
mise exec -- cargo test --release --manifest-path native/pulso_codec/Cargo.toml > /tmp/pulso-cost-rust-checks.log 2>&1 || { tail -80 /tmp/pulso-cost-rust-checks.log; exit 1; }
tail -4 /tmp/pulso-cost-rust-checks.log
