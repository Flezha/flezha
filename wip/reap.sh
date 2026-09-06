#!/usr/bin/env bash
# Find, and optionally kill, what a work session left behind.
#
# Runs at session start, at every heartbeat, and at every item boundary.
# Reports by default. `--kill` kills only the categories below, never anything
# else, and prints what it killed so the supervisor can log it.
#
#   wip/reap.sh            report
#   wip/reap.sh --kill     report and kill the reapable categories
#
# NEVER killed: peer agent sessions, epmd, editor language servers, anything
# belonging to another project on this machine. NEVER `docker compose down`:
# another project may share the compose project name. Stop containers by name.
set -uo pipefail

KILL=0
[ "${1:-}" = "--kill" ] && KILL=1
found=0

section() { printf '\n== %s ==\n' "$1"; }

# 1. Real zombies. A zombie cannot be killed; its parent must reap it. Report
#    the parent so the supervisor knows which session is leaking.
section "zombies"
z=$(ps -eo stat,pid,ppid,etime,comm | awk 'NR==1 || $1 ~ /^Z/')
if [ "$(printf '%s\n' "$z" | wc -l)" -gt 1 ]; then
  printf '%s\n' "$z"
  printf 'parents holding them:\n'
  ps -eo stat,ppid --no-headers | awk '$1 ~ /^Z/ {print $2}' | sort -u |
    while read -r p; do ps -o pid,etime,cmd --no-headers -p "$p" 2>/dev/null; done
  found=1
else
  echo "none"
fi

# 2. Build, test and benchmark processes. Anything here that outlived its job is
#    ours to kill: nothing in this repository is meant to run unattended.
section "build / test / benchmark"
pat='beam\.smp|cargo|rustc|criterion|mix |mvn |java .*surefire|phpunit|pytest|rake|jest|node .*test'
b=$(ps -eo pid,etime,cmd --no-headers | grep -E "$pat" | grep -v -E 'grep|reap\.sh')
if [ -n "$b" ]; then
  printf '%s\n' "$b"
  found=1
  if [ "$KILL" = 1 ]; then
    printf '%s\n' "$b" | awk '{print $1}' | while read -r p; do
      cmd=$(ps -o cmd= -p "$p" 2>/dev/null | cut -c1-70)
      kill -TERM "$p" 2>/dev/null && echo "killed $p  $cmd"
    done
  fi
else
  echo "none"
fi

# 3. Polling loops and bare sleeps. A worker is told never to leave one; this is
#    the check that assumes it did anyway.
section "polling loops"
l=$(ps -eo pid,etime,cmd --no-headers | grep -E 'until \[|while \[|sleep [0-9]+' |
    grep -v -E 'grep|reap\.sh')
if [ -n "$l" ]; then
  printf '%s\n' "$l"
  found=1
  if [ "$KILL" = 1 ]; then
    printf '%s\n' "$l" | awk '{print $1}' | while read -r p; do
      kill -TERM "$p" 2>/dev/null && echo "killed $p (polling loop)"
    done
  fi
else
  echo "none"
fi

# 4. Containers. Reported only: which of these are ours is a judgement, and the
#    sibling project's containers must survive.
section "containers"
if command -v docker >/dev/null 2>&1; then
  docker ps --format '{{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || echo "docker not answering"
else
  echo "docker not installed"
fi

# 5. Load, so a quiet snapshot is not read as a quiet box.
section "load"
uptime

[ "$found" = 1 ] && [ "$KILL" = 0 ] && echo "
Something is left over. Rule each line logical or leaked BY NAME, then rerun
with --kill for the categories above. Log every kill in wip/LOG.md."
exit 0
