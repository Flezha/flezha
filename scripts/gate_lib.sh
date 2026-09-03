#!/usr/bin/env bash
# Shared by scripts/format-apply.sh and scripts/format-check.sh. Sourced, never
# executed: the caller sets `set -euo pipefail`, calls `gate_run` once per check
# and ends with `gate_report`.
#
# The rule this file holds: a check has three outcomes and none of them is
# another. A tool that is not installed removes its own check from the report
# and nothing else; a tool that ran and says the tree is wrong is a finding; a
# tool that ran and never reached a verdict is neither, and gets its own list.
# Conflating the first two is what made the old gate unreadable — it tested
# every tool BEFORE running any check, so one absent formatter ended the run and
# said nothing about the six languages behind it. Six unformatted files reached
# a commit whose author had read "the gate exits 0" and never seen a line of its
# output. Conflating the last two names a defect in a tree that does not have
# one, and sends the reader to fix what is already correct.
#
# Exit status, so the distinction survives across a process boundary:
#   0  everything ran, nothing to report
#   1  a check ran and found something
#   2  nothing failed, but a tool was missing so some check did not run
#   3  a tool ran and did not complete, so its check has no answer
# When several apply the status is 3, else 1, else 2: an unanswered check is
# reported before one that answered "no", because until it runs the silence
# behind it means nothing.

gate_unavailable=()
gate_failed=()
gate_errored=()

# A check's output is streamed AND kept, because a `says:` rule below has to
# read it. One file per process, rewritten by each check.
gate_log=$(mktemp "${TMPDIR:-/tmp}/gate_lib.XXXXXX")
trap 'rm -f "$gate_log"' EXIT

# gate_run [--nested] [--finding <rule>] <label> [<tool>:<how to install it> ...] \
#          -- <command> [args...]
#
# The install hint is the old preflight's useful half and is kept; only the
# abort is gone. <command> may be a shell function, so a check that is a
# pipeline still gets exactly one label.
#
# `--finding` says how to tell "this tool found something" from "this tool did
# not complete", which a status alone cannot: a formatter that reports drift and
# one that crashes both exit non-zero. Two forms:
#   status:<n>[,<n>...]  the tool documents a distinct code for a finding; every
#                        other non-zero status is a failure to complete.
#   says:<ere>           the tool returns the same status either way, so the
#                        verdict it prints for a finding decides. Matched
#                        against stdout and stderr merged, because tools differ
#                        on which one carries the verdict.
# Without `--finding` a check has no finding state and every non-zero status is
# a failure to complete — which is what a formatter in write mode needs.
#
# `--nested` says the command is itself a gate_lib gate, so its status is read
# as one of ours: 2 is a missing tool and 3 a tool that did not complete. It has
# to be asked for, because a plain tool's 2 is not a gate's 2 — prettier, ruff
# and rubocop all use exit 2 for "the tool errored", which they declare with
# `--finding status:1`.
gate_run() {
  local label nested=0 skip=0 tool rc=0 finding= out
  while [[ $1 == --nested || $1 == --finding ]]; do
    if [[ $1 == --nested ]]; then nested=1; shift; else finding=$2; shift 2; fi
  done
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
  "$@" 2>&1 | tee "$gate_log" || rc=$?
  out=$(<"$gate_log")
  if ((rc == 0)); then
    return 0
  elif ((nested)); then
    case $rc in
      2) gate_unavailable+=("$label — see its own summary above") ;;
      3) gate_errored+=("$label — see its own summary above") ;;
      *) gate_failed+=("$label") ;;
    esac
  elif [[ $finding == status:* && ",${finding#status:}," == *",$rc,"* ]] ||
    [[ $finding == says:* && $out =~ ${finding#says:} ]]; then
    gate_failed+=("$label")
  else
    gate_errored+=("$label — exit $rc")
  fi
}

# gate_report <heading for the checks that ran and found something>
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
  if ((${#gate_errored[@]})); then
    printf '\nNO ANSWER — the tool ran and did not complete:\n'
    printf '  %s\n' "${gate_errored[@]}"
    status=3
  fi
  ((status)) || printf '\nevery check ran and had nothing to report\n'
  return "$status"
}
