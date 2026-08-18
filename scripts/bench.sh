#!/bin/bash
# Flezha benchmark runner — wrapper for `mix bench`.
#
# Picks the SDK runtime, transport, workload shape, and dimensions.
# Everything is optional; defaults match the merge-blocking CI gate
# (rust + uds + regression shape, 100 operatives x 100 msg).
#
# Quick examples:
#   ./scripts/bench.sh                                # rust + uds, default dims
#   ./scripts/bench.sh -r java                        # java SDK over uds
#   ./scripts/bench.sh -t quic                        # rust SDK over quic
#   ./scripts/bench.sh -r java -t quic -n 100 -m 1000 # java + quic, bigger run
#   ./scripts/bench.sh --shape entity --pool-sizes 1,8
#
# Compare two runs:
#   ./scripts/bench.sh -r rust -n 100 -m 1000 | tee bench-rust.txt
#   ./scripts/bench.sh -r java -n 100 -m 1000 | tee bench-java.txt
#   diff bench-rust.txt bench-java.txt
#
# Prerequisites:
#   - rust runtime: nothing extra (test/apps/rust_runner is built on demand)
#   - java runtime: run `mix test.java_sdk` once to build the test jar
#   - quic transport: certs auto-generated at app_data/certs/ on first run
#
# Full reference: docs/benchmarking.md
set -e

RUNTIME="rust"
TRANSPORT="uds"
SHAPE=""
POOL_SIZES=""
NUM_OPERATIVES=100
MESSAGES_PER_OPERATIVE=100
WARMUP=""
MIX_ENV_OVERRIDE="${MIX_ENV:-prod}"

usage() {
    cat <<'EOF'
Usage: ./scripts/bench.sh [OPTIONS]

Options:
  -r, --runtime <rust|java>          SDK runtime (default: rust)
  -t, --transport <uds|quic>         Wire transport (default: uds)
  -s, --shape <regression|entity|dynamic>
                                     Bench shape (default: regression).
                                     `entity` ignores -n/-m and uses the
                                     8x4x8x20k stress workload. `dynamic` drives
                                     the dynamic_entity operative (lazy
                                     per-key Entities, 8 keys/operative), honors -n/-m.
      --pool-sizes <list>            Comma-separated worker-pool sizes for
                                     --shape entity (default: 1,8)
  -n, --num-operatives <N>               Operative count for regression shape (default: 100)
  -m, --messages-per-operative <N>       Messages per operative for regression shape
                                     (default: 100)
  -w, --warmup <N>                   Untimed warmup messages per operative before
                                     the measured phase (default: 10, 0 disables)
  -h, --help                         Show this help

Switching combinations are independent:
  -r picks the SDK (rust or java)
  -t picks the wire (uds or quic)
  -s picks the workload (regression or entity)
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--runtime)         RUNTIME="$2"; shift 2 ;;
        -t|--transport)       TRANSPORT="$2"; shift 2 ;;
        -s|--shape)           SHAPE="$2"; shift 2 ;;
        --pool-sizes)         POOL_SIZES="$2"; shift 2 ;;
        -n|--num-operatives)      NUM_OPERATIVES="$2"; shift 2 ;;
        -m|--messages-per-operative) MESSAGES_PER_OPERATIVE="$2"; shift 2 ;;
        -w|--warmup)          WARMUP="$2"; shift 2 ;;
        -h|--help)            usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

case "$RUNTIME" in rust|java) ;; *)
    echo "Error: --runtime must be 'rust' or 'java', got '$RUNTIME'"; exit 1 ;;
esac
case "$TRANSPORT" in uds|quic) ;; *)
    echo "Error: --transport must be 'uds' or 'quic', got '$TRANSPORT'"; exit 1 ;;
esac
if [[ -n "$SHAPE" ]]; then
    case "$SHAPE" in regression|entity|dynamic) ;; *)
        echo "Error: --shape must be 'regression', 'entity', or 'dynamic', got '$SHAPE'"; exit 1 ;;
    esac
fi

ARGS=(--runtime "$RUNTIME")
[[ -n "$SHAPE" ]] && ARGS+=(--shape "$SHAPE")
[[ -n "$WARMUP" ]] && ARGS+=(--warmup "$WARMUP")

if [[ "$SHAPE" == "entity" ]]; then
    [[ -n "$POOL_SIZES" ]] && ARGS+=(--pool-sizes "$POOL_SIZES")
    echo "Running bench: runtime=$RUNTIME transport=$TRANSPORT shape=entity pool-sizes=${POOL_SIZES:-1,8}"
else
    ARGS+=(--num-operatives "$NUM_OPERATIVES" --messages-per-operative "$MESSAGES_PER_OPERATIVE")
    echo "Running bench: runtime=$RUNTIME transport=$TRANSPORT operatives=$NUM_OPERATIVES msg/operative=$MESSAGES_PER_OPERATIVE"
fi

export FLEZHA_TRANSPORT="$TRANSPORT"
export MIX_ENV="$MIX_ENV_OVERRIDE"
exec mix bench "${ARGS[@]}"
