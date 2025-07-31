#!/bin/bash
set -e

# Set defaults
LOG_LEVEL=${LOG_LEVEL:-info}
PORT=${PORT:-8080}

# Start the application with environment-based configuration
exec bin/agent start --mode="$AGENT_MODE"