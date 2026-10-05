#!/bin/bash
set -euo pipefail
export MIX_ENV=test PULSO_NIF_FORCE_BUILD=true
export ERL_FLAGS="+S 4:4 +SDcpu 4"
mix compile --warnings-as-errors > .auto/compile.out 2>&1 || { tail -80 .auto/compile.out; exit 1; }
/usr/bin/time -l mix run --no-compile .auto/workload.exs 2> .auto/time.out
python3 - <<'PY'
import re
s = open('.auto/time.out').read()
m = re.search(r'(\d+)\s+maximum resident set size', s)
if m:
    print('METRIC peak_rss_mb=' + str(int(m[1]) / 1048576))
else:
    print(s)
    raise SystemExit('missing peak RSS')
PY
