#!/usr/bin/env bash
# Fails when a retired module path is still named anywhere in the tree.
#
# Eight modules were renamed or folded into their parent because the last
# segment named the wrong thing:
#
#   Flezha.State.Registry                         -> Flezha.State.Paths
#   Flezha.AuthZ.Store                            -> Flezha.AuthZ.DetsStore
#   Flezha.SDK.Backoff                            -> Flezha.SDK.Jitter
#   Flezha.Service.Parser                         -> Flezha.Service.Identificator
#   Flezha.Plugin.Provider.Metrics                -> Flezha.State.Metrics
#   Flezha.Plugin.Provider.Kafka.Telemetry        -> Flezha.Plugin.Provider.Kafka
#   Flezha.Plugin.Provider.OperativeForward.Telemetry
#                                                 -> Flezha.Plugin.Provider.OperativeForward
#   Flezha.Observability.OtpBackend.Source
#                                                 -> Flezha.Observability.OtpBackend.Poller
#   Flezha.Convergence.Backoff                    -> Flezha.Config.Backoff
#
# `mix compile` catches a stale CODE reference; nothing catches one in a
# moduledoc, a spec, a runbook or a config comment, and a plugin whose provider
# module cannot be loaded is reported as `:running` anyway
# (lib/flezha/plugin/extension/table.ex). This gate is the only layer that sees
# those surfaces.
#
# Each pattern is the full retired path with a trailing word boundary, so a live
# module that merely starts with one — `Flezha.AuthZ.StoresTest` — is out of
# scope by construction rather than by exemption. A last segment on its own
# (`Store`, `Metrics`, `Telemetry`) is deliberately NOT matched: those words are
# ordinary and every live spelling would be a false positive.
#
# Scope is `git ls-files`, so .git/, target/, _build/ and openspec/changes/
# (which holds the archive) are excluded by being untracked or ignored; dist/
# build artifacts and binaries are excluded explicitly. Takes no arguments.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

exec python3 - <<'PY'
import re
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, "scripts")
import naming_gate

RETIRED = (
    "Flezha.State.Registry",
    "Flezha.AuthZ.Store",
    "Flezha.SDK.Backoff",
    "Flezha.Service.Parser",
    "Flezha.Plugin.Provider.Metrics",
    "Flezha.Plugin.Provider.Kafka.Telemetry",
    "Flezha.Plugin.Provider.OperativeForward.Telemetry",
    "Flezha.Observability.OtpBackend.Source",
    "Flezha.Convergence.Backoff",
)

PATTERNS = [re.compile(re.escape(path) + r"\b") for path in RETIRED]

sys.exit(naming_gate.walk(
    check=naming_gate.pattern_check(PATTERNS),
    prefilter=lambda text: "Flezha." in text,
    # The gate's own files carry the retired names as data.
    self_files={"scripts/check-module-naming.sh", ".github/workflows/naming.yml"},
    record_tag="module-naming-record",
    headline="retired module path still named "
             "(see scripts/check-module-naming.sh for the nine and their replacements):",
    hint="A live reference takes the new name. A document citing the old name as "
         "history wraps it in a `module-naming-record` block.",
    ok_message="module naming OK",
))
PY
