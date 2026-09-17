#!/usr/bin/env bash
# Format every language; with `--check`, report what is unformatted and write
# nothing. The list of formatters lives here and nowhere else:
# `python3 scripts/naming_gate.py --run formatters` is what a gate run calls,
# and .github/workflows/format.yml calls that runner rather than re-spelling
# the list.
#
# Every formatter is attempted and every verdict is reported. A tool that is
# not installed takes its own language out of the report and nothing else; the
# summary lists unavailable tools apart from unformatted trees, and so does the
# exit status. The four codes and why the distinction is the point of this
# script are documented once, at the top of scripts/naming_gate.py, which is
# the runner every gate in this repository answers to. `gate_run` and
# `gate_report` below are that contract in shell, for the one caller that is
# itself a driver of several tools rather than a single check.
#
# Rust and Elixir use the toolchain on PATH. Every other SDK is developed in a
# repository of its own and formatted by that repository's CI.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

gate_unavailable=()
gate_failed=()
gate_errored=()

# A check's output is streamed AND kept, because a `says:` rule below has to
# read it. One file per process, rewritten by each check.
gate_log=$(mktemp "${TMPDIR:-/tmp}/format_apply.XXXXXX")
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
# `--nested` says the command answers on the same contract, so its status is read
# as one of ours: 2 is a missing tool and 3 a tool that did not complete. It has
# to be asked for, because a plain tool's 2 is not a gate's 2: a formatter that
# exits 2 for "the tool errored" declares its finding with `--finding status:1`.
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

# What each formatter does when it finds drift, as opposed to failing for its
# own reasons. rustfmt and mix return 1 either way,
# so the verdict each prints for drift, and for nothing else, decides. In write
# mode a formatter has nothing to find: no rule is declared, and gate_run then
# reads every non-zero status as the tool failing.
declare -A drift=()
if [[ "${1:-}" == "--check" ]]; then
  rust=(-- --check) elixir=(--check-formatted)
  drift=([rust]='says:Diff in '
    [elixir]='says:format failed due to --check-formatted')
else
  rust=() elixir=()
fi

# The nightly is pinned to the one .github/workflows/format.yml installs.
# rustfmt.toml's `imports_granularity`, `group_imports` and `ignore` are
# nightly-only options, and two nightlies are two style opinions; bump this
# with the workflow and rustfmt.toml together, as rust-toolchain.toml says.
gate_run --finding "${drift[rust]:-}" "cargo fmt" cargo:"install Rust via rustup" -- \
  cargo +nightly-2026-05-07 fmt --all "${rust[@]}"
gate_run --finding "${drift[elixir]:-}" "mix format" mix:"install Elixir" -- \
  mix format "${elixir[@]}"

gate_report "UNFORMATTED — run scripts/format-apply.sh:"
