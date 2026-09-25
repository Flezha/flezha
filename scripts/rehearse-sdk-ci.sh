#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Anuar Alfetahe
#
# SPDX-License-Identifier: MPL-2.0
#
# Rehearse every SDK repository's own CI before the core release it runs
# against exists. Each SDK is exported from its committed HEAD into a repository
# of its own, which is what a CI checkout sees, and each job of its workflow runs
# under `act` in the GitHub runner image, unmodified. The release is built from
# this tree for the host's triple, and every github.com/flezha repository cargo
# reaches (the core at its tag, and the submodules it fetches with it) is a
# local bare clone, the core tagged here, so nothing is published or pushed.
#
#   scripts/rehearse-sdk-ci.sh [--release <staged dir>] [sdk ...]
#
# `--release` takes a directory `build-dist.sh --stage-release` wrote, instead of
# building one. Logs are kept under ${TMPDIR:-/tmp}/flezha-sdk-ci-rehearsal.
#
# What cannot run here is printed as NOT REHEARSED, never passed over in
# silence: the macOS and Windows arms, and the jobs that need their output.
# Exit codes are the runner's contract, which naming_gate.py documents.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO_ROOT="$PWD"

FINDING=1
TOOL_MISSING=2
NO_ANSWER=3

IMAGE="flezha-sdk-ci-rehearsal:24.04"
ORG_GIT_URL="https://github.com/flezha/"
LOGS="${TMPDIR:-/tmp}/flezha-sdk-ci-rehearsal"

# Jobs this host cannot rehearse, and why.
declare -A NOT_REHEARSED=(
  ["node/packages"]="packs all five addons; the macOS and Windows ones are built on those runners"
  ["ruby/platform-gems"]="rb-sys-dock inside act cannot reach the local mirror; the release build above built the x86_64-linux gem with the same tool"
  ["ruby/install-without-a-toolchain"]="installs the platform-gems output"
)

RELEASE=""
SDKS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) RELEASE="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) SDKS+=("$1"); shift ;;
  esac
done
(( ${#SDKS[@]} )) || SDKS=(elixir java node php python ruby rust)

for tool in docker act; do
  command -v "${tool}" >/dev/null 2>&1 || { echo "rehearse-sdk-ci: ${tool} is not on PATH" >&2; exit "${TOOL_MISSING}"; }
done

# The rehearsal is of what would be pushed. Uncommitted work is in some
# artefacts and not in others, so it would rehearse a tree nobody can push.
dirty="$(git status --porcelain --ignore-submodules=none)"
if [[ -n "${dirty}" ]]; then
  echo "rehearse-sdk-ci: the tree has uncommitted changes; commit them first:" >&2
  echo "${dirty}" >&2
  exit "${NO_ANSWER}"
fi

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
rm -rf "${LOGS}"
mkdir -p "${LOGS}"

version="$(awk -F'"' '/^version = / { print $2; exit }' Cargo.toml)"
triple="$(rustc -vV | awk '/^host:/ { print $2 }')"
mirror="${work}/mirror"
git clone -q --bare "${REPO_ROOT}" "${mirror}/flezha"
git -C "${mirror}/flezha" tag "v${version}" HEAD
while read -r key url; do
  path="$(git config -f .gitmodules --get "${key%.url}.path")"
  git clone -q --bare "${path}" "${mirror}/${url##*/}"
done < <(git config -f .gitmodules --get-regexp '\.url$')

if [[ -z "${RELEASE}" ]]; then
  echo "== building the ${triple} release against a local v${version} (log: ${LOGS}/release.log)"
  RELEASE="${work}/release"
  if ! { FLEZHA_GIT_MIRROR="${mirror}" scripts/build-dist.sh --host --triples "${triple}" &&
         scripts/build-dist.sh --stage-release "${RELEASE}" --triples "${triple}"; } > "${LOGS}/release.log" 2>&1; then
    tail -30 "${LOGS}/release.log" >&2
    echo "rehearse-sdk-ci: the release did not build; nothing was rehearsed" >&2
    exit "${NO_ANSWER}"
  fi
fi
RELEASE="$(cd "${RELEASE}" && pwd)"
[[ -f "${RELEASE}/SHA256SUMS.txt" ]] || { echo "rehearse-sdk-ci: ${RELEASE} is not a staged release" >&2; exit "${NO_ANSWER}"; }

# act's runner image, plus what GitHub's ubuntu-24.04 image carries and act's
# does not, and jobs lean on: Maven, and a stable Rust on PATH.
docker build -q --pull -t "${IMAGE}" - > /dev/null <<'DOCKERFILE'
FROM catthehacker/ubuntu:act-24.04
RUN apt-get update && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH=/root/.cargo/bin:${PATH}
DOCKERFILE

act_args=(
  -P "ubuntu-latest=${IMAGE}"
  --pull=false
  # The macOS and Windows arms of a matrix have no image here.
  --matrix os:ubuntu-latest
  --rm
  --artifact-server-path "${work}/artifacts"
  --container-options "-v ${RELEASE}:/core-release:ro -v ${mirror}:/mirror:ro"
  --env CORE_RELEASE=/core-release
  --env CARGO_NET_GIT_FETCH_WITH_CLI=true
  --env GIT_CONFIG_COUNT=2
  --env "GIT_CONFIG_KEY_0=url.file:///mirror/.insteadOf"
  --env "GIT_CONFIG_VALUE_0=${ORG_GIT_URL}"
  --env GIT_CONFIG_KEY_1=safe.directory
  --env "GIT_CONFIG_VALUE_1=*"
)

passed=()
failed=()
skipped=()
for sdk in "${SDKS[@]}"; do
  src="sdk/${sdk}"
  [[ -d "${src}" ]] || { echo "rehearse-sdk-ci: no SDK at ${src}" >&2; exit "${NO_ANSWER}"; }
  if ! git -C "${src}" ls-files --error-unmatch '.github/workflows/*' >/dev/null 2>&1; then
    skipped+=("${sdk}: has no CI workflow, so nothing runs for it at publication")
    continue
  fi

  repo="${work}/${sdk}"
  mkdir -p "${repo}"
  git -C "${src}" archive HEAD | tar -x -C "${repo}"
  git -C "${repo}" init -q
  git -C "${repo}" add -A
  git -C "${repo}" -c user.name=rehearsal -c user.email=rehearsal@localhost commit -qm rehearsal

  # One line per job: its id, then the non-Linux runners any arm of it asks for.
  while IFS='|' read -r job others; do
    if [[ -n "${NOT_REHEARSED[${sdk}/${job}]:-}" ]]; then
      skipped+=("${sdk}/${job}: ${NOT_REHEARSED[${sdk}/${job}]}")
      continue
    fi
    [[ -z "${others}" ]] || skipped+=("${sdk}/${job}: its ${others} arms")
    log="${LOGS}/${sdk}-${job}.log"
    echo "== ${sdk}/${job}"
    if act push -C "${repo}" -j "${job}" "${act_args[@]}" < /dev/null > "${log}" 2>&1; then
      passed+=("${sdk}/${job}")
    else
      tail -30 "${log}"
      failed+=("${sdk}/${job} (log: ${log})")
    fi
  done < <(python3 - "${repo}/.github/workflows" <<'PY'
import pathlib
import sys

import yaml

for path in sorted(pathlib.Path(sys.argv[1]).glob("*.y*ml")):
    for job_id, job in (yaml.safe_load(path.read_text()).get("jobs") or {}).items():
        text = repr(job.get("runs-on")) + repr(job.get("strategy"))
        others = [os for os in ("macos", "windows") if os in text]
        print(f"{job_id}|{' and '.join(others)}")
PY
)
done

echo
echo "rehearsed against ${RELEASE} and a local v${version} of the core"
printf '  passed        %s\n' "${passed[@]}"
(( ${#skipped[@]} == 0 )) || printf '  NOT REHEARSED %s\n' "${skipped[@]}"
if (( ${#failed[@]} > 0 )); then
  printf '  FAILED        %s\n' "${failed[@]}"
  exit "${FINDING}"
fi
(( ${#passed[@]} > 0 )) || { echo "rehearse-sdk-ci: no job ran" >&2; exit "${NO_ANSWER}"; }
