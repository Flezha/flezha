#!/bin/bash
set -e

# Flezha Dev Environment Setup Script
# ====================================
# This script builds and starts the dev Docker environment,
# then opens a shell inside the controller container with the project
# compiled and ready to use.
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - Run this script from the project root directory
#
# Usage:
#   ./scripts/dev.sh [command] [args...]
#
# Commands:
#   build    - Build images and start containers
#   up       - Start containers (without rebuilding)
#   down     - Stop and remove containers
#   shell    - Open a shell in the running controller container
#   iex      - Open an IEX session inside the running controller container
#   logs     - Follow controller container logs
#   clean    - Remove build cache volumes and stop containers
#   rebuild  - Full clean rebuild: down + remove volumes + build + up
#   status   - Show container status
#   release  - Build an Elixir release inside the running container
#   cargo    - Run a cargo command inside the running container
#              e.g.  ./scripts/dev.sh cargo build --workspace
#   help     - Show this help message
#
# After the environment is running, you can:
#
#   Start the controller (with IEX):
#     ./scripts/dev.sh iex
#
#   Or start the controller via release:
#     ./scripts/dev.sh release
#     _build/dev/rel/flezha/bin/flzvm start
#
#   Build the Rust workspace (libflezha, SDK, CLI, test app):
#     ./scripts/dev.sh cargo build --workspace
#
#   Start a compute engine (in a separate terminal/shell):
#     cargo run -p rust_executor
#
#   Run the benchmark:
#     mix bench --num-relays 1000 --messages-per-relay 1000
#
#   Kafka UI is available at: http://localhost:8080/

COMPOSE_FILE="docker/dev/docker-compose.yml"
CONTAINER_NAME="flezha_controller_dev"
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
    echo "Building dev containers..."
    docker compose -f "$COMPOSE_FILE" build
}

cmd_up() {
    ensure_env_file
    ensure_network
    echo "Starting dev containers..."
    docker compose -f "$COMPOSE_FILE" up -d
    echo ""
    echo "Dev environment is running."
    echo "Run './scripts/dev.sh shell' to open a shell in the controller container."
}

cmd_down() {
    echo "Stopping dev containers..."
    docker compose -f "$COMPOSE_FILE" down
}

cmd_shell() {
    echo "Opening shell in $CONTAINER_NAME..."
    docker exec -it "$CONTAINER_NAME" bash
}

cmd_iex() {
    echo "Opening IEX session in $CONTAINER_NAME..."
    docker exec -it "$CONTAINER_NAME" \
        iex --name flezha@127.0.0.1 --cookie flezha_dev_cookie -S mix
}

cmd_logs() {
    docker compose -f "$COMPOSE_FILE" logs -f controller
}

cmd_clean() {
    echo "Removing build cache volumes..."
    docker compose -f "$COMPOSE_FILE" down -v
    echo "Build cache cleared. Run './scripts/dev.sh up' to rebuild."
}

cmd_rebuild() {
    echo "Full rebuild: stopping, clearing volumes, rebuilding..."
    docker compose -f "$COMPOSE_FILE" down -v
    ensure_env_file
    ensure_network
    docker compose -f "$COMPOSE_FILE" build
    docker compose -f "$COMPOSE_FILE" up -d
    echo ""
    echo "Dev environment rebuilt and running."
}

cmd_status() {
    docker compose -f "$COMPOSE_FILE" ps
}

cmd_release() {
    echo "Building Elixir release inside $CONTAINER_NAME..."
    docker exec "$CONTAINER_NAME" bash -c "mix release --overwrite"
}

cmd_cargo() {
    echo "Running: cargo $*"
    docker exec -it "$CONTAINER_NAME" bash -c ". \"\$HOME/.cargo/env\" && cargo $*"
}

cmd_help() {
    # Print the usage section from the header comment
    sed -n '/^# Usage:/,/^[^#]/p' "$0" | head -n -1 | sed 's/^# \?//'
}

# Main
COMMAND="${1:-build}"
shift || true

case "$COMMAND" in
    build)   cmd_build ;;
    up)      cmd_up ;;
    down)    cmd_down ;;
    shell)   cmd_shell ;;
    iex)     cmd_iex ;;
    logs)    cmd_logs ;;
    clean)   cmd_clean ;;
    rebuild) cmd_rebuild ;;
    status)  cmd_status ;;
    release) cmd_release ;;
    cargo)   cmd_cargo "$@" ;;
    help)    cmd_help ;;
    *)
        echo "Unknown command: $COMMAND"
        cmd_help
        exit 1
        ;;
esac
