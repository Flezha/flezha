#!/usr/bin/env bash
# Find, and kill, what a work session left behind.
#
# Runs at session start, at every heartbeat, at every item boundary and before
# a session or loop ends.
#
#   wip/reap.sh              report
#   wip/reap.sh --kill       also kill orphans: leftovers no live session,
#                            terminal or editor owns. Safe at any time.
#   wip/reap.sh --kill-mine  also kill what this session started (its MCP
#                            servers excepted) and stop this repository's
#                            containers. Only while no worker of it runs.
#
# Every process is ruled by its ancestry, so ownership is never a guess:
#   mine       under the Claude session running this script
#   peer       under another Claude session
#   container  inside a container
#   user       under a terminal, editor or desktop the owner runs
#   orphan     none of these: leaked by a session that is gone
#   new        started after the snapshot; never killed
# An interactive Claude session counts as user: its terminal owns it.
# NEVER killed: peer, container and user processes, epmd, language servers.
# Containers are stopped by name, never with `docker compose down`: another
# project may share the compose project name.
set -uo pipefail

MODE=report
case "${1:-}" in
  --kill) MODE=orphans ;;
  --kill-mine) MODE=mine ;;
esac
ROOT=$(cd "$(dirname "$0")/.." && pwd)
found=0

declare -A PARENT TTY COMM
while read -r pid ppid tty comm; do
  PARENT[$pid]=$ppid TTY[$pid]=$tty COMM[$pid]=$comm
done < <(ps -eo pid=,ppid=,tty=,comm=)

# This script, its shell and the session above them are never candidates.
declare -A SELF
ME=""
p=$$
while [ -n "${PARENT[$p]:-}" ] && [ "$p" -gt 1 ]; do
  SELF[$p]=1
  [ -z "$ME" ] && [ "${COMM[$p]}" = claude ] && ME=$p
  p=${PARENT[$p]}
done

HOSTS=' code idea gnome-shell gnome-terminal- tmux: screen sshd kitty alacritty konsole xterm wezterm-gui ptyxis '

owner() {
  local p=$1 claude=0 container=0 user=0
  [ -z "${PARENT[$p]:-}" ] && { echo new; return; }
  while [ -n "${PARENT[$p]:-}" ] && [ "$p" -gt 1 ]; do
    [ "$p" = "$ME" ] && { echo mine; return; }
    case "${COMM[$p]}" in
      claude) [ "$p" != "$1" ] && claude=1 ;;
      containerd-shim*) container=1 ;;
    esac
    case "$HOSTS" in *" ${COMM[$p]} "*) user=1 ;; esac
    [ "${TTY[$p]}" != "?" ] && user=1
    p=${PARENT[$p]}
  done
  if [ "$claude" = 1 ]; then echo peer
  elif [ "$container" = 1 ]; then echo container
  elif [ "$user" = 1 ]; then echo user
  else echo orphan; fi
}

section() { printf '\n== %s ==\n' "$1"; }

# List the processes matching a pattern with their owner, and kill the ones the
# mode allows. $2 = 1 spares MCP servers even when they are ours.
reap() {
  local pat=$1 spare_mcp=${2:-0} any=0 pid etime cmd who
  while read -r pid etime cmd; do
    [ -n "${SELF[$pid]:-}" ] && continue
    who=$(owner "$pid")
    printf '%-9s %8s %10s  %s\n' "$who" "$pid" "$etime" "${cmd:0:150}"
    any=1
    # A test's peer BEAM is detached from its parent, so it reads as an
    # orphan while its run is live; a gone session's leftovers are older.
    if [ "$who" = orphan ] && [[ $etime =~ ^0?[0-4]:[0-9]{2}$ ]]; then
      echo "  spared $pid (orphan under 5 min old)"
      continue
    fi
    case "$MODE/$who" in
      orphans/orphan | mine/orphan) ;;
      mine/mine) [ "$spare_mcp" = 1 ] && [[ $cmd == *mcp* ]] && continue ;;
      *) continue ;;
    esac
    kill -TERM "$pid" 2>/dev/null && echo "  killed $pid ($who)"
  done < <(ps -eo pid=,etime=,args= | grep -E "$pat" | grep -v -E 'grep|reap\.sh')
  [ "$any" = 1 ] && found=1 || echo "none"
}

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

# 2. Agent sessions. A headless one whose spawning session is gone is an orphan.
section "agent sessions"
reap '^ *[0-9]+ +[-0-9:]+ +claude( |$)'

# 3. Browsers and their drivers: Playwright, its MCP servers, headless Chrome.
section "browsers / playwright"
reap 'ms-playwright|playwright|headless_shell|--remote-debugging-p' 1

# 4. Build, test and benchmark processes. Nothing in this repository is meant
#    to run unattended.
section "build / test / benchmark"
reap 'beam\.smp|cargo|rustc|criterion|mix |mvn |java .*surefire|phpunit|pytest|rake|jest|node .*test'

# 5. Polling loops and bare sleeps. A worker is told never to leave one; this is
#    the check that assumes it did anyway.
section "polling loops"
reap 'until \[|while \[|while true|sleep [0-9]+'

# 6. Containers. Ours are those composed from this repository; the rest belong
#    to sibling projects and are only reported.
section "containers"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  c=$(docker ps --format '{{.Names}}\t{{.RunningFor}}\t{{.Label "com.docker.compose.project.working_dir"}}')
  if [ -n "$c" ]; then
    found=1
    printf '%s\n' "$c" | while IFS=$'\t' read -r name age dir; do
      case "$dir" in "$ROOT"|"$ROOT"/*) who=mine ;; *) who=other ;; esac
      printf '%-9s %-30s %-16s %s\n' "$who" "$name" "$age" "$dir"
      [ "$MODE/$who" = mine/mine ] && docker stop "$name" >/dev/null && echo "  stopped $name"
    done
  else
    echo "none running"
  fi
else
  echo "docker not answering"
fi

# 7. CPU over the last second, so a quiet snapshot is not read as a quiet box.
#    Every line of ours here must be explainable by name.
section "cpu"
uptime
top -b -n 2 -d 1 -w 200 | awk '/^ *PID/ {n++} n==2' | head -9 | cut -c1-150

[ "$found" = 1 ] && [ "$MODE" = report ] && echo "
Something is running. Orphans: rerun with --kill. This session's leftovers,
once no worker of it runs: --kill-mine. Log every kill in wip/LOG.md."
exit 0
