#!/usr/bin/env bash
#
# The controller release archive: its platforms, its name, packing it, and the
# smoke test every archive passes on its own platform before it is published.
# CI's release workflow and `make release-archive` both come through here, and
# scripts/build-dist.sh asks it for the names it stages.
#
#   scripts/release-archive.sh platforms
#   scripts/release-archive.sh name <platform>
#   scripts/release-archive.sh pack <release dir>          the host's archive
#   scripts/release-archive.sh smoke <archive> <runner>    the Rust test runner
#
# Written for bash 3.2, the one macOS ships: no associative arrays.

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${REPO_ROOT}/_build/.dist/controller"
PLATFORMS="linux-x86_64 macos-arm64 macos-x86_64"

die() { printf 'release-archive: %s\n' "$*" >&2; exit 1; }

archive_name() {
  case " ${PLATFORMS} " in
    *" $1 "*) printf 'flezha-%s-%s.tar.gz\n' "$("${REPO_ROOT}/scripts/manifest_pin.sh" "${REPO_ROOT}/mix.exs" version)" "$1" ;;
    *) die "$1 is not one of: ${PLATFORMS}" ;;
  esac
}

host_platform() {
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) echo linux-x86_64 ;;
    Darwin-arm64) echo macos-arm64 ;;
    Darwin-x86_64) echo macos-x86_64 ;;
    *) die "no release archive is built for $(uname -s) $(uname -m)" ;;
  esac
}

# The release's own tmp/ and storage/ hold the logs, the cookie and the state of
# any local run, so they never travel. Nor does releases/COOKIE: mix writes one
# into every release, nothing here reads it, and a published copy would be the
# same in every download. COPYFILE_DISABLE keeps macOS's tar from adding `._`
# metadata files.
pack() {
  local rel="${1%/}"
  [[ "$(basename "${rel}")" == flezha && -x "${rel}/bin/flz" ]] ||
    die "${rel} is not a release directory named flezha with bin/flz"
  local platform name
  platform="$(host_platform)"
  name="$(archive_name "${platform}")"
  mkdir -p "${OUT_DIR}"
  COPYFILE_DISABLE=1 tar -czf "${OUT_DIR}/${name}" --exclude='flezha/tmp' \
    --exclude='flezha/storage' --exclude='flezha/releases/COOKIE' -C "$(dirname "${rel}")" flezha
  echo "${OUT_DIR}/${name}"
}

# Unpack the archive, start its controller with everything it writes under one
# temporary directory, prove a runner connects and is placed a relay, then stop
# it. Authorization is off: the smoke test is about the binaries, and a runner
# then joins its namespace by naming it. The directory sits under /tmp because
# the socket path must stay inside macOS's 103-byte limit.
smoke() {
  [[ -f "$1" ]] || die "no archive at $1"
  [[ -x "$2" ]] || die "no runner binary at $2"
  local archive runner
  archive="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
  runner="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"

  # Global, not local: the EXIT trap runs after this function has returned.
  runner_pid="" started=""
  work="$(mktemp -d /tmp/flz-smoke.XXXXXX)"
  rel="${work}/flezha"

  # shellcheck disable=SC2329 # run by the trap
  finish() {
    local status=$?
    set +e
    exec 3<&- 4>&-
    [[ -z "${runner_pid}" ]] || { kill "${runner_pid}"; wait "${runner_pid}"; } 2>/dev/null
    [[ -z "${started}" ]] || (cd "${rel}" && bin/flz controller stop >/dev/null 2>&1)
    # Whatever still runs from the unpacked release: a controller that would
    # not stop, or its run_erl a moment after it did. The path is mktemp's own.
    pkill -KILL -f "${work}/" 2>/dev/null
    if (( status != 0 )); then
      for log in "${work}"/runner.err "${rel}"/tmp/log/*; do
        [[ -f "${log}" ]] && { echo "--- ${log}" >&2; tail -n 40 "${log}" >&2; }
      done
    fi
    rm -rf "${work}"
  }
  trap finish EXIT
  step() { printf '==> %s\n' "$1"; }

  tar -xzf "${archive}" -C "${work}"
  export FLEZHA_STATE_DIR="${work}/state" FLEZHA_SOCKET_FILES_PATH="${work}/s"
  export FLEZHA_AUTHZ_ENABLED=false
  cd "${rel}"

  step "the controller starts"
  bin/flz controller start --mode daemon || die "the controller did not start"
  started=1
  bin/flz boot await --timeout 120 || die "the controller did not become ready"

  step "flz runner check reaches it"
  bin/flz runner check || die "flz runner check did not reach the controller"

  step "a runner connects and is placed a relay"
  mkfifo "${work}/in" "${work}/out"
  FLEZHA_RUNNER_ID_PATH="${work}/runner-id" \
    FLEZHA_ADMIN_SCRIPT='[{"verb":"Relay.start","args":[{"id":"smoke","factory_id":"do_nothing"}]}]' \
    "${runner}" smoke <"${work}/in" >"${work}/out" 2>"${work}/runner.err" &
  runner_pid=$!
  # The runner exits when its stdin closes, so holding fd 4 keeps it running.
  exec 4>"${work}/in" 3<"${work}/out"
  # The runner logs to stdout too; its answer is the line that starts `admin`.
  local line="" answer=""
  while IFS= read -r -t 120 line <&3; do
    [[ "${line}" == "admin "* ]] && { answer="${line}"; break; }
  done
  [[ -n "${answer}" ]] || die "the runner exited or answered nothing within 120 s"
  [[ "${answer}" == "admin Relay.start ok"* ]] || die "the runner's relay start answered: ${answer}"
  [[ "$(bin/flz runner list -n smoke --json)" != "[]" ]] || die "flz runner list shows no runner"
  bin/flz relay list -n smoke | grep -qw smoke || die "flz relay list does not show the relay"

  step "the runner leaves and the controller stops"
  exec 4>&-
  wait "${runner_pid}" || die "the runner did not exit cleanly"
  runner_pid=""
  bin/flz controller stop || die "flz controller stop failed"
  started=""
  echo "smoke: $(basename "${archive}") passed"
}

case "${1:-}" in
  platforms) echo "${PLATFORMS}" ;;
  name) archive_name "${2:?a platform}" ;;
  pack) pack "${2:?a release directory}" ;;
  smoke) smoke "${2:?an archive}" "${3:?a runner binary}" ;;
  *) die "usage: $0 platforms | name <platform> | pack <release dir> | smoke <archive> <runner>" ;;
esac
