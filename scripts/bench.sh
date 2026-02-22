#!/bin/bash
set -e

# Flezha Benchmark Script
# =======================
# Wrapper for `mix bench` with configurable parameters.
#
# Usage:
#   ./scripts/bench.sh [--num-processors N] [--messages-per-processor N]
#
# Defaults:
#   --num-processors 100
#   --messages-per-processor 100

NUM_PROCESSORS=100
MESSAGES_PER_PROCESSOR=100

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
        --help|-h)
            echo "Usage: ./scripts/bench.sh [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -n, --num-processors N          Number of processors (default: 100)"
            echo "  -m, --messages-per-processor N   Messages per processor (default: 100)"
            echo "  -h, --help                       Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run './scripts/bench.sh --help' for usage."
            exit 1
            ;;
    esac
done

echo "Running benchmark: $NUM_PROCESSORS processors, $MESSAGES_PER_PROCESSOR messages each"
mix bench --num-processors "$NUM_PROCESSORS" --messages-per-processor "$MESSAGES_PER_PROCESSOR"
