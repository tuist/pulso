#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=1
mise exec -- mix compile --warnings-as-errors > /tmp/pulso-cost-compile.log 2>&1 || { tail -80 /tmp/pulso-cost-compile.log; exit 1; }
mise exec -- mix test .auto/cost_bench.exs --seed 42 --timeout 300000
