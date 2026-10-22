#!/bin/bash
# Flezha benchmark runner — wrapper for `mix bench`.
#
# Picks the SDK runtime, transport, workload shape, and dimensions.
# Everything is optional; the defaults are the merge-blocking CI gate's runtime,
# transport and shape (rust + uds + regression). Its dimensions are not: CI runs
# -n 100 -m 10000, where this script defaults to 100 relays x 100 msg.
#
# Quick examples:
#   ./scripts/bench.sh                                # rust + uds, default dims
#   ./scripts/bench.sh -r java                        # java SDK over uds
#   ./scripts/bench.sh -t quic                        # rust SDK over quic
#   ./scripts/bench.sh -r java -t quic -n 100 -m 1000 # java + quic, bigger run
#   ./scripts/bench.sh --shape cell --pool-sizes 1,8
#   FLEZHA_NODE_WORKERS=4 ./scripts/bench.sh -r node  # node on four dispatch loops
#
# A figure comparable with published latency and durability numbers:
#   ./scripts/bench.sh --governor performance --strict --durability local_path \
#     --shape tier --latency --rate 20000 --payload-bytes 256
#
# Every run opens with a header naming the runtime and its dispatch model, the
# machine, and the artifact it launched, followed by any preflight warning that
# qualifies the number (a non-performance CPU governor, a contended box, a
# dirty tree, a skipped build). The run always proceeds.
#
# The headline throughput is the END-TO-END window (first input to last
# response); the receive-only window is reported beside it. They differ by the
# send window and are not interchangeable.
#
# Every run also writes local_dev/bench/reports/bench-<runtime>-<shape>-<run_id>.json
# with every header, warning and metric field. That report is the machine
# contract — CI reads metrics.throughput_msg_per_sec from it.
#
# FLEZHA_BENCH_LOG sets the logger level for the run (default: warning).
#
# Compare two runs:
#   ./scripts/bench.sh -r rust -n 100 -m 1000 | tee bench-rust.txt
#   ./scripts/bench.sh -r java -n 100 -m 1000 | tee bench-java.txt
#   diff bench-rust.txt bench-java.txt
#
# Prerequisites:
#   - none: `mix bench` runs each runtime's own `bench` build command from
#     test/apps/<runtime>_runner/conformance.json before it measures, because
#     an artifact that exists is not an artifact that is current. Set
#     FLEZHA_BENCH_SKIP_BUILD=1 to skip it.
#   - quic transport: certs auto-generated at storage/app/certs/ on first run
#   - node: FLEZHA_NODE_WORKERS selects the dispatch topology (0 = the main
#     event loop, the default). It is inherited by the runner, and the bench
#     header reports which one a number was taken under.
#
# Full reference: docs/internal/operations/benchmarking.md
set -e

RUNTIME="rust"
TRANSPORT="uds"
SHAPE=""
POOL_SIZES=""
NUM_RELAYS=100
MESSAGES_PER_RELAY=100
WARMUP=""
VIA=""
LATENCY=""
RATE=""
PAYLOAD_BYTES=""
STRICT=""
DURABILITY=""
GOVERNOR=""
MIX_ENV_OVERRIDE="${MIX_ENV:-prod}"

usage() {
    cat <<'EOF'
Usage: ./scripts/bench.sh [OPTIONS]

Options:
  -r, --runtime <name>               SDK runtime (default: rust). Any SDK with
                                     a test/apps/<name>_runner/conformance.json
  -t, --transport <uds|quic>         Wire transport (default: uds)
  -s, --shape <name>                 Bench shape (default: regression).
                                     `cell` ignores -n/-m and uses the
                                     8x4x8x20k stress workload. `dynamic` drives
                                     the dynamic_cell relay (lazy
                                     per-key Cells, 8 keys/relay), honors -n/-m.
                                     Also tier, sleeping, timer and send; see
                                     `mix help bench`
      --pool-sizes <list>            Comma-separated worker-pool sizes for
                                     --shape cell (default: 1,8)
  -n, --num-relays <N>               Relay count for regression shape (default: 100)
  -m, --messages-per-relay <N>       Messages per relay for regression shape
                                     (default: 100)
  -w, --warmup <N>                   Untimed warmup messages per relay before
                                     the measured phase (default: 10, 0 disables)
      --via <cast|host_inject>       How inputs reach a relay (default: cast);
                                     host_inject goes through Plugin.Host.inject/5
      --latency                      Report p50/p95/p99/p99.9/max latency from
                                     stamped inputs
      --rate <N>                     Send N inputs/s in total instead of one
                                     burst (needs --via cast; not for cell)
      --payload-bytes <N>            Pad every input to exactly N bytes (not
                                     for sleeping); the size is always reported
      --durability <none|local_path|postgres|s3>
                                     Remote durability tier (default: none).
                                     local_path defaults to a store the bench
                                     owns and clears; postgres and s3 read their
                                     settings from FLEZHA_DURABILITY_* variables
      --strict                       Refuse to measure when a box or build
                                     warning holds (governor, load, MIX_ENV,
                                     dirty tree, skipped/fallback/debug build)
      --governor <name>              Switch every CPU to this governor for the
                                     run and restore it on exit. Needs cpupower
                                     and a cached sudo credential (sudo -v)
  -h, --help                         Show this help

Environment:
  FLEZHA_BENCH_LOG        Logger level for the run (default: warning)
  FLEZHA_BENCH_SKIP_BUILD 1 skips the runtime's bench build; the run warns
  FLEZHA_DURABILITY_BACKEND  Defaults to `none` here; --durability sets it

Every run writes local_dev/bench/reports/bench-<runtime>-<shape>-<run_id>.json, which
carries the header, the warnings and every metric. It is what CI reads.

Switching combinations are independent:
  -r picks the SDK (rust or java)
  -t picks the wire (uds or quic)
  -s picks the workload (see --shape)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--runtime)         RUNTIME="$2"; shift 2 ;;
        -t|--transport)       TRANSPORT="$2"; shift 2 ;;
        -s|--shape)           SHAPE="$2"; shift 2 ;;
        --pool-sizes)         POOL_SIZES="$2"; shift 2 ;;
        -n|--num-relays)      NUM_RELAYS="$2"; shift 2 ;;
        -m|--messages-per-relay) MESSAGES_PER_RELAY="$2"; shift 2 ;;
        -w|--warmup)          WARMUP="$2"; shift 2 ;;
        --via)                VIA="$2"; shift 2 ;;
        --latency)            LATENCY=1; shift ;;
        --rate)               RATE="$2"; shift 2 ;;
        --payload-bytes)      PAYLOAD_BYTES="$2"; shift 2 ;;
        --strict)             STRICT=1; shift ;;
        --durability)         DURABILITY="$2"; shift 2 ;;
        --governor)           GOVERNOR="$2"; shift 2 ;;
        -h|--help)            usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# The runtimes are the SDK test apps that exist, not a list kept in step by
# hand: a new SDK is benchable the moment its manifest is there.
RUNTIMES=()
for manifest in "$(dirname "$0")/../test/apps/"*_runner/conformance.json; do
    [[ -e "$manifest" ]] || continue
    app="$(basename "$(dirname "$manifest")")"
    RUNTIMES+=("${app%_runner}")
done
if [[ ! " ${RUNTIMES[*]} " == *" $RUNTIME "* ]]; then
    echo "Error: --runtime must be one of: ${RUNTIMES[*]} — got '$RUNTIME'"; exit 1
fi
case "$TRANSPORT" in uds|quic) ;; *)
    echo "Error: --transport must be 'uds' or 'quic', got '$TRANSPORT'"; exit 1 ;;
esac
# `mix bench` owns the list of shapes and refuses an unknown one by name.

# `mix bench` would drop a non-integer silently, so the script refuses it.
require_positive_int() {
    if [[ -n "$2" && ! "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: $1 must be a positive integer, got '$2'"; exit 1
    fi
}
require_positive_int --rate "$RATE"
require_positive_int --payload-bytes "$PAYLOAD_BYTES"
if [[ -n "$DURABILITY" ]]; then
    case "$DURABILITY" in none|local_path|postgres|s3) ;; *)
        echo "Error: --durability must be one of: none local_path postgres s3 — got '$DURABILITY'"; exit 1 ;;
    esac
fi

ARGS=(--runtime "$RUNTIME")
[[ -n "$SHAPE" ]] && ARGS+=(--shape "$SHAPE")
[[ -n "$WARMUP" ]] && ARGS+=(--warmup "$WARMUP")
[[ -n "$VIA" ]] && ARGS+=(--via "$VIA")
[[ -n "$LATENCY" ]] && ARGS+=(--latency)
[[ -n "$RATE" ]] && ARGS+=(--rate "$RATE")
[[ -n "$PAYLOAD_BYTES" ]] && ARGS+=(--payload-bytes "$PAYLOAD_BYTES")
[[ -n "$STRICT" ]] && ARGS+=(--strict)

if [[ "$SHAPE" == "cell" ]]; then
    [[ -n "$POOL_SIZES" ]] && ARGS+=(--pool-sizes "$POOL_SIZES")
else
    ARGS+=(--num-relays "$NUM_RELAYS" --messages-per-relay "$MESSAGES_PER_RELAY")
fi

export FLEZHA_TRANSPORT="$TRANSPORT"
export MIX_ENV="$MIX_ENV_OVERRIDE"
# A prod-env controller refuses to boot without an explicit durability decision.
# A benchmark's state is disposable, so the opt-out is the honest default here;
# set the variable yourself to measure a real tier's cost.
if [[ -n "$DURABILITY" ]]; then
    export FLEZHA_DURABILITY_BACKEND="$DURABILITY"
else
    export FLEZHA_DURABILITY_BACKEND="${FLEZHA_DURABILITY_BACKEND:-none}"
fi
# A `local_path` store with no root gets one the bench owns, which the run
# clears before it boots. A root the user set is never cleared.
if [[ "$FLEZHA_DURABILITY_BACKEND" == "local_path" && -z "${FLEZHA_DURABILITY_PATH:-}" ]]; then
    export FLEZHA_DURABILITY_PATH="storage/app/bench/durability"
    export FLEZHA_BENCH_DURABILITY_OWNED=1
fi
# The run boots its own controller state, so a dev controller already running
# from this checkout neither holds the state lock against it nor hands it its
# declared relays and installed plugin packages.
export FLEZHA_STATE_DIR="${FLEZHA_STATE_DIR:-storage/app/bench/state}"
export FLEZHA_PROCESS_HUB_REGISTRY_ROOT="${FLEZHA_PROCESS_HUB_REGISTRY_ROOT:-storage/app/bench/process_hub}"
export FLEZHA_PLUGIN_ROOT="${FLEZHA_PLUGIN_ROOT:-storage/app/bench/plugins}"

# Each CPU's governor is recorded and put back on every exit, a failed or
# interrupted run included. Nothing is changed until every check has passed.
GOVERNOR_FILES=(/sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor)
SAVED_GOVERNORS=()

# shellcheck disable=SC2329 # run by the EXIT trap
restore_governors() {
    local i cpu
    for i in "${!SAVED_GOVERNORS[@]}"; do
        cpu="${GOVERNOR_FILES[$i]#/sys/devices/system/cpu/cpu}"
        cpu="${cpu%%/*}"
        sudo -n cpupower -c "$cpu" frequency-set -g "${SAVED_GOVERNORS[$i]}" >/dev/null ||
            echo "Warning: could not restore cpu$cpu to ${SAVED_GOVERNORS[$i]}" >&2
    done
}

if [[ -n "$GOVERNOR" ]]; then
    if [[ ! -r "${GOVERNOR_FILES[0]}" ]]; then
        echo "Error: --governor needs ${GOVERNOR_FILES[0]}, which this host does not have"; exit 1
    fi
    available="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_available_governors)"
    if [[ ! " $available " == *" $GOVERNOR "* ]]; then
        echo "Error: --governor must be one of: $available — got '$GOVERNOR'"; exit 1
    fi
    if ! command -v cpupower >/dev/null; then
        echo "Error: --governor needs cpupower (linux-tools / kernel-tools package)"; exit 1
    fi
    if ! sudo -n true 2>/dev/null; then
        echo "Error: --governor needs a cached sudo credential; run 'sudo -v' first"; exit 1
    fi

    for file in "${GOVERNOR_FILES[@]}"; do
        SAVED_GOVERNORS+=("$(cat "$file")")
    done
    trap restore_governors EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM

    sudo -n cpupower frequency-set -g "$GOVERNOR" >/dev/null
    echo "CPU governor: ${SAVED_GOVERNORS[0]} -> $GOVERNOR for this run."
    echo "  restored on exit; by hand: sudo cpupower frequency-set -g ${SAVED_GOVERNORS[0]}"
fi

# Not `exec`: the governor trap has to run after mix exits.
status=0
mix bench "${ARGS[@]}" || status=$?
exit "$status"
