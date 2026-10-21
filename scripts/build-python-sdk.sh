#!/usr/bin/env bash
# Build the Python SDK and its test app: the native veneer, and the extension
# module the package loads.
#
# `--release` builds the veneer in release, which is what the bench profile
# wants; the default is a debug veneer, which is what the e2e wants (it builds
# in seconds and the runner is not what a conformance run measures).
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk="$root/sdk/python"

profile=debug
cargo_args=()
if [[ "${1:-}" == "--release" ]]; then
  profile=release
  cargo_args+=(--release)
fi

echo "==> building flz_core_ffi ($profile) with the python-extension feature"
cargo build -p flz_core_ffi --features python-extension "${cargo_args[@]}"

# `from flezha import flz_core_ffi` looks for a module of that name inside the
# package and calls `PyInit_flz_core_ffi`, which the veneer exports. The cdylib
# is that module under another name — no second artifact, exactly as the `.node`
# addon and the Ruby extension are.
suffix="$(python3 -c 'import sysconfig; print(sysconfig.get_config_var("EXT_SUFFIX"))')"
echo "==> installing the $profile veneer as flezha/flz_core_ffi$suffix"
cp "$root/_build/.target/$profile/libflz_core_ffi.so" "$sdk/src/flezha/flz_core_ffi$suffix"

# A checkout-local interpreter, so the e2e app and the unit suite run against
# this SDK's declared dependencies without installing anything system-wide —
# the same posture `scripts/build-ruby-sdk.sh` takes with `gem install`, and
# what the conformance manifest's command template names.
venv="$sdk/.venv"
if [[ ! -x "$venv/bin/python" ]]; then
  echo "==> creating the checkout-local virtual environment"
  "${FLEZHA_PYTHON:-python3}" -m venv "$venv"
fi
echo "==> installing package dependencies"
"$venv/bin/pip" install --quiet --disable-pip-version-check msgpack

# Put THIS checkout's package on that interpreter's path, so `import flezha`
# from the e2e app, the worked example or an ad-hoc script resolves to the
# source that was just built. A path file rather than `pip install -e .`: the
# editable install would run maturin and build the extension a second time,
# which is the one thing this script has already done.
site="$("$venv/bin/python" -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')"
echo "$sdk/src" > "$site/flezha-checkout.pth"

echo "==> python SDK ready ($("$venv/bin/python" --version))"
