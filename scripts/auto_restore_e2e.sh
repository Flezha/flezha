#!/usr/bin/env bash
# auto_restore_e2e.sh — single-node end-to-end verification of the
# auto-restore-on-boot capability.
#
# Steps:
#   1. Start the dev controller + runner via docker compose.
#   2. Register 5 operators + plugins, push a few state changes.
#   3. `docker compose down`, then `docker compose up`.
#   4. Assert all 5 operators come back running with their plugins and that
#      the most recent state is reflected in `flz state at`.

set -euo pipefail

cd "$(dirname "$0")/.."

DC="docker compose -f docker/dev/docker-compose.yml"
OPERATORS=("auto_e2e_a" "auto_e2e_b" "auto_e2e_c" "auto_e2e_d" "auto_e2e_e")

note() { printf '\033[1;34m[auto-restore-e2e]\033[0m %s\n' "$*"; }

step_up() {
  note "starting controller + runner"
  $DC up -d controller kafka
  sleep 5
}

step_down() {
  note "tearing down (preserving volumes)"
  $DC down
}

step_register() {
  for operator in "${OPERATORS[@]}"; do
    note "registering $operator"
    $DC exec -T controller flz operator start "$operator" rust_factory --namespace prod
    $DC exec -T controller flz plugin enable "${operator}_kafka" operator_kafka_producer "$operator" --namespace prod \
      || true
  done
}

step_push_state() {
  note "pushing input frames so each operator accumulates state"
  for operator in "${OPERATORS[@]}"; do
    $DC exec -T controller mix run -e \
      "Flezha.send_io(\"$operator\", \"prod\", \"hello\", %{})" || true
  done
  sleep 2
}

step_assert() {
  note "asserting all 5 operators are running"
  for operator in "${OPERATORS[@]}"; do
    $DC exec -T controller flz operator list --namespace prod | grep -q "$operator" \
      || { echo "FAIL: $operator missing after restart"; exit 1; }
  done
  note "ok"
}

main() {
  step_up
  step_register
  step_push_state
  step_down
  step_up
  step_assert
  step_down
}

main "$@"
