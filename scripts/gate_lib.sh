#!/usr/bin/env bash
# Shared by scripts/format-apply.sh and scripts/format-check.sh. Sourced, never
# executed: the caller sets `set -euo pipefail`, calls `gate_run` once per check
# and ends with `gate_report`.
#
# The rule this file holds: a tool that is not installed removes its own check
# from the report and nothing else. "Tool unavailable" and "check failed" are
# different findings and are listed separately, because conflating them is what
# made the old gate unreadable — it tested every tool BEFORE running any check,
# so one absent formatter ended the run and said nothing about the six
# languages behind it. Six unformatted files reached a commit whose author had
# read "the gate exits 0" and never seen a line of its output.
#
# Exit status, so the distinction survives across a process boundary:
#   0  everything ran, nothing to report
#   1  a check ran and failed
#   2  nothing failed, but a tool was missing so some check did not run

gate_unavailable=()
gate_failed=()

# gate_run [--nested] <label> [<tool>:<how to install it> ...] -- <command> [args...]
#
# The install hint is the old preflight's useful half and is kept; only the
# abort is gone. <command> may be a shell function, so a check that is a
# pipeline still gets exactly one label. `--nested` says the command is itself
# a gate_lib gate, so its exit 2 is read as "a tool was missing" rather than
# being flattened into a failure. It has to be asked for: prettier, ruff and
# rubocop all use exit 2 for "the tool errored", which is a failure.
gate_run() {
  local label nested=0 skip=0 tool rc=0
  [[ $1 == --nested ]] && { nested=1; shift; }
  label=$1
  shift
  while [[ $1 != "--" ]]; do
    tool=${1%%:*}
    command -v "$tool" >/dev/null 2>&1 ||
      { gate_unavailable+=("$label — no $tool: ${1#*:}"); skip=1; }
    shift
  done
  shift
  if ((skip)); then
    echo "==> $label — NOT RUN, see the summary"
    return 0
  fi
  echo "==> $label"
  "$@" || rc=$?
  if ((rc == 0)); then
    return 0
  elif ((nested && rc == 2)); then
    gate_unavailable+=("$label — see its own summary above")
  else
    gate_failed+=("$label")
  fi
}

# gate_report <heading for the checks that ran and failed>
# Returns the status documented at the top of this file; the caller ends on it.
gate_report() {
  local status=0
  if ((${#gate_unavailable[@]})); then
    printf '\nNOT RUN — the tool is not installed here:\n'
    printf '  %s\n' "${gate_unavailable[@]}"
    status=2
  fi
  if ((${#gate_failed[@]})); then
    printf '\n%s\n' "$1"
    printf '  %s\n' "${gate_failed[@]}"
    status=1
  fi
  ((status)) || printf '\nevery check ran and had nothing to report\n'
  return "$status"
}
