#!/bin/bash
set -e

# Flezha Benchmark Script
# =======================
# Wrapper for `mix bench` with configurable parameters.
#
# Usage:
#   ./scripts/bench.sh [--num-processors N] [--messages-per-processor N] \
#                      [--transport uds|quic] [--runtime rust|java]
#
# Defaults:
#   --num-processors 100
#   --messages-per-processor 100
#   --transport uds
#   --runtime rust

NUM_PROCESSORS=100
MESSAGES_PER_PROCESSOR=100
TRANSPORT="uds"
RUNTIME="rust"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --num-processors|-n)
            NUM_PROCESSORS="$2"
            shift 2
            ;;
        --messages-per-processor|-m)
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
            echo "  -n, --num-processors N          Number of processors (default: 100)"
            echo "  -m, --messages-per-processor N  Messages per processor (default: 100)"
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

echo "Running benchmark: runtime=$RUNTIME, transport=$TRANSPORT, $NUM_PROCESSORS processors, $MESSAGES_PER_PROCESSOR messages each"
export FLEZHA_TRANSPORT="$TRANSPORT"
mix bench \
    --num-processors "$NUM_PROCESSORS" \
    --messages-per-processor "$MESSAGES_PER_PROCESSOR" \
    --runtime "$RUNTIME"
