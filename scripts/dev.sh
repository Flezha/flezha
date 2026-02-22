#!/bin/bash
set -e

# Flezha Dev Environment Setup Script
# ====================================
# This script builds and starts the dev Docker environment,
# then opens a shell inside the agent container with the project
# compiled and ready to use.
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - Run this script from the project root directory
#
# Usage:
#   ./scripts/dev.sh [command]
#
# Commands:
#   build    - Build images and start containers
#   up       - Start containers (without rebuilding)
#   down     - Stop and remove containers
#   shell    - Open a shell in the running agent container
#   logs     - Follow agent container logs
#   help     - Show this help message
#
# After the environment is running, you can:
#
#   Start the agent (with IEX):
#     iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix
#
#   Or start the agent via release:
#     mix release
#     _build/dev/rel/flezha/bin/flz agent start --mode=attached_iex
#
#   Start a compute engine (in a separate terminal/shell):
#     cd test/apps/rust_compute_engine
#     cargo run --target-dir=./target
#
#   Run the benchmark:
#     mix bench --num-processors 1000 --messages-per-processor 1000
#
#   Kafka UI is available at: http://localhost:8080/

COMPOSE_FILE="docker/dev/docker-compose.yml"
CONTAINER_NAME="flezha_agent_dev"
NETWORK_NAME="flezha_network"

ensure_env_file() {
    if [ ! -f .env ]; then
        echo "Creating .env file from .env.example..."
        cp .env.example .env
    fi
}

ensure_network() {
    if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
        echo "Creating docker network '$NETWORK_NAME'..."
        docker network create "$NETWORK_NAME"
    fi
}

cmd_build() {
    ensure_env_file
    ensure_network
    echo "Building and starting dev containers..."
    docker compose -f "$COMPOSE_FILE" up --build -d
    echo ""
    echo "Dev environment is running."
    echo "Run './scripts/dev.sh shell' to open a shell in the agent container."
}

cmd_up() {
    ensure_env_file
    ensure_network
    echo "Starting dev containers..."
    docker compose -f "$COMPOSE_FILE" up -d
    echo ""
    echo "Dev environment is running."
    echo "Run './scripts/dev.sh shell' to open a shell in the agent container."
}

cmd_down() {
    echo "Stopping dev containers..."
    docker compose -f "$COMPOSE_FILE" down
}

cmd_shell() {
    echo "Opening shell in $CONTAINER_NAME..."
    docker exec -it "$CONTAINER_NAME" bash
}

cmd_logs() {
    docker compose -f "$COMPOSE_FILE" logs -f agent
}

cmd_help() {
    # Print the usage section from the header comment
    sed -n '/^# Usage:/,/^[^#]/p' "$0" | head -n -1 | sed 's/^# \?//'
}

# Main
COMMAND="${1:-build}"

case "$COMMAND" in
    build) cmd_build ;;
    up)    cmd_up ;;
    down)  cmd_down ;;
    shell) cmd_shell ;;
    logs)  cmd_logs ;;
    help)  cmd_help ;;
    *)
        echo "Unknown command: $COMMAND"
        cmd_help
        exit 1
        ;;
esac
