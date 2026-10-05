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
# Every run opens with a header naming the runtime and its dispatch model, the
# machine, and the artifact it launched, followed by any preflight warning that
# qualifies the number (a non-performance CPU governor, a contended box, a
# dirty tree, a skipped build). The run always proceeds.
#
# The headline throughput is the END-TO-END window (first input to last
# response); the receive-only window is reported beside it. They differ by the
# send window and are not interchangeable.
#
# Every run also writes bench/reports/bench-<runtime>-<shape>-<run_id>.json
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
#   - quic transport: certs auto-generated at app_data/certs/ on first run
#   - node: FLEZHA_NODE_WORKERS selects the dispatch topology (0 = the main
#     event loop, the default). It is inherited by the runner, and the bench
#     header reports which one a number was taken under.
#
# Full reference: docs/operations/benchmarking.md
set -e

RUNTIME="rust"
TRANSPORT="uds"
SHAPE=""
POOL_SIZES=""
NUM_RELAYS=100
MESSAGES_PER_RELAY=100
WARMUP=""
MIX_ENV_OVERRIDE="${MIX_ENV:-prod}"

usage() {
    cat <<'EOF'
Usage: ./scripts/bench.sh [OPTIONS]

Options:
  -r, --runtime <name>               SDK runtime (default: rust). Any SDK with
                                     a test/apps/<name>_runner/conformance.json
  -t, --transport <uds|quic>         Wire transport (default: uds)
  -s, --shape <regression|cell|dynamic>
                                     Bench shape (default: regression).
                                     `cell` ignores -n/-m and uses the
                                     8x4x8x20k stress workload. `dynamic` drives
                                     the dynamic_cell relay (lazy
                                     per-key Cells, 8 keys/relay), honors -n/-m.
      --pool-sizes <list>            Comma-separated worker-pool sizes for
                                     --shape cell (default: 1,8)
  -n, --num-relays <N>               Relay count for regression shape (default: 100)
  -m, --messages-per-relay <N>       Messages per relay for regression shape
                                     (default: 100)
  -w, --warmup <N>                   Untimed warmup messages per relay before
                                     the measured phase (default: 10, 0 disables)
  -h, --help                         Show this help

Environment:
  FLEZHA_BENCH_LOG        Logger level for the run (default: warning)
  FLEZHA_BENCH_SKIP_BUILD 1 skips the runtime's bench build; the run warns
  FLEZHA_DURABILITY_BACKEND  Defaults to `none` here; the run warns

Every run writes bench/reports/bench-<runtime>-<shape>-<run_id>.json, which
carries the header, the warnings and every metric. It is what CI reads.

Switching combinations are independent:
  -r picks the SDK (rust or java)
  -t picks the wire (uds or quic)
  -s picks the workload (regression or cell)
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
case "$TRANSPORT" in uds|quic|native) ;; *)
    echo "Error: --transport must be 'uds', 'quic' or 'native', got '$TRANSPORT'"; exit 1 ;;
esac
# The native transport is the in-process Elixir SDK; rust/java cannot use it.
if [[ "$TRANSPORT" == "native" && "$RUNTIME" != "elixir" ]]; then
    echo "Error: --transport native requires --runtime elixir (it is the in-process SDK)"; exit 1
fi
if [[ -n "$SHAPE" ]]; then
    case "$SHAPE" in regression|cell|dynamic) ;; *)
        echo "Error: --shape must be 'regression', 'cell', or 'dynamic', got '$SHAPE'"; exit 1 ;;
    esac
fi

ARGS=(--runtime "$RUNTIME")
[[ -n "$SHAPE" ]] && ARGS+=(--shape "$SHAPE")
[[ -n "$WARMUP" ]] && ARGS+=(--warmup "$WARMUP")

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
export FLEZHA_DURABILITY_BACKEND="${FLEZHA_DURABILITY_BACKEND:-none}"
exec mix bench "${ARGS[@]}"
