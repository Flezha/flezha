#!/usr/bin/env bash
# Production lines per repository, the measure a campaign's line budget is held
# to. Prints one line per repository or crate and a total. What counts as
# production is defined once, in scripts/production_code.py, which the ceiling
# gate reads too.
set -euo pipefail
exec python3 "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)/scripts/production_code.py"
