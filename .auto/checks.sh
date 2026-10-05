#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=1
mise exec -- mix test --seed 42 > /tmp/pulso-cost-checks.log 2>&1 || { tail -80 /tmp/pulso-cost-checks.log; exit 1; }
tail -4 /tmp/pulso-cost-checks.log
