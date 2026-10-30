#!/usr/bin/env bash
#
# What a cross-build through zig needs, installed when missing: cargo-zigbuild,
# zig in a venv of its own when none is on PATH, and the rustup target of each
# triple named. Prints the venv's bin directory, for the caller to put on PATH.
#
#   export PATH="$(scripts/ensure-zig.sh aarch64-apple-darwin x86_64-apple-darwin):${PATH}"
#
# scripts/build-dist.sh and the CI job of the `@macos` gate row come through here.

set -euo pipefail

ZIG_VENV="${HOME}/.local/share/flezha-zig-venv"

if ! command -v cargo-zigbuild >/dev/null 2>&1; then
  echo "installing cargo-zigbuild (one-time, ~30s)" >&2
  cargo install cargo-zigbuild --locked >&2
fi

if ! command -v zig >/dev/null 2>&1 && [[ ! -x "${ZIG_VENV}/bin/zig" ]]; then
  echo "creating persistent zig venv at ${ZIG_VENV}" >&2
  python3 -m venv "${ZIG_VENV}"
  "${ZIG_VENV}/bin/pip" install --quiet ziglang >&2
  cat > "${ZIG_VENV}/bin/zig" <<EOF
#!/bin/bash
exec "${ZIG_VENV}/bin/python" -m ziglang "\$@"
EOF
  chmod +x "${ZIG_VENV}/bin/zig"
fi

for t in "$@"; do
  if ! rustup target list --installed 2>/dev/null | grep -qx "${t}"; then
    echo "rustup target add ${t}" >&2
    rustup target add "${t}" >&2
  fi
done

echo "${ZIG_VENV}/bin"
