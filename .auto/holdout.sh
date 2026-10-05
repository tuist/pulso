#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=true
export ERL_FLAGS="+S 4:4 +SDcpu 4"
mix run --no-compile .auto/holdout.exs
