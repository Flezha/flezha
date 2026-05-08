#!/bin/bash
set -e

# Flezha Benchmark Script
# =======================
# Wrapper for `mix bench` with configurable parameters.
#
# Usage:
#   ./scripts/bench.sh [--num-relays N] [--messages-per-relay N] \
#                      [--transport uds|quic] [--runtime rust|java]
#
# Defaults:
#   --num-relays 100
#   --messages-per-relay 100
#   --transport uds
#   --runtime rust

NUM_PROCESSORS=100
MESSAGES_PER_PROCESSOR=100
TRANSPORT="uds"
RUNTIME="rust"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --num-relays|-n)
            NUM_PROCESSORS="$2"
            shift 2
            ;;
        --messages-per-relay|-m)
            MESSAGES_PER_PROCESSOR="$2"
            shift 2
            ;;
        --transport|-t)
            TRANSPORT="$2"
            if [[ "$TRANSPORT" != "uds" && "$TRANSPORT" != "quic" ]]; then
                echo "Error: --transport must be 'uds' or 'quic', got '$TRANSPORT'"
                exit 1
            fi
            shift 2
            ;;
        --runtime|-r)
            RUNTIME="$2"
            if [[ "$RUNTIME" != "rust" && "$RUNTIME" != "java" ]]; then
                echo "Error: --runtime must be 'rust' or 'java', got '$RUNTIME'"
                exit 1
            fi
            shift 2
            ;;
        --help|-h)
            echo "Usage: ./scripts/bench.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -n, --num-relays N          Number of relays (default: 100)"
            echo "  -m, --messages-per-relay N  Messages per relay (default: 100)"
            echo "  -t, --transport uds|quic        Transport to use (default: uds)"
            echo "  -r, --runtime rust|java         SDK runtime to benchmark (default: rust)"
            echo "  -h, --help                      Show this help message"
            echo ""
            echo "Examples:"
            echo "  ./scripts/bench.sh --runtime rust -n 100 -m 1000 | tee bench-rust.txt"
            echo "  ./scripts/bench.sh --runtime java -n 100 -m 1000 | tee bench-java.txt"
            echo "  diff bench-rust.txt bench-java.txt"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run './scripts/bench.sh --help' for usage."
            exit 1
            ;;
    esac
done

echo "Running benchmark: runtime=$RUNTIME, transport=$TRANSPORT, $NUM_PROCESSORS relays, $MESSAGES_PER_PROCESSOR messages each"
export FLEZHA_TRANSPORT="$TRANSPORT"
mix bench \
    --num-relays "$NUM_PROCESSORS" \
    --messages-per-relay "$MESSAGES_PER_PROCESSOR" \
    --runtime "$RUNTIME"
