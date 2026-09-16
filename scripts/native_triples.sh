# The five supported target triples, and how each one is spelled: as a staged
# directory, as a library extension and prefix, and as a published release
# asset. Sourced, never executed.
#
# `scripts/build-native.sh` fetches assets by these names and
# `scripts/build-dist.sh` uploads them under the same ones, so the two sides of
# the fetch contract are written once, here. The same table also decides the
# resource path the Java runtime loads from — see
# sdk/java/flezha-sdk-native/.../NativeLibraryLoader.java, whose SUPPORTED set
# is the directory column.

ALL_TRIPLES=(
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
  "x86_64-pc-windows-gnu"
)

declare -A TRIPLE_TO_DIR=(
  ["x86_64-unknown-linux-gnu"]="linux-x64"
  ["aarch64-unknown-linux-gnu"]="linux-arm64"
  ["x86_64-apple-darwin"]="macos-x64"
  ["aarch64-apple-darwin"]="macos-arm64"
  ["x86_64-pc-windows-gnu"]="windows-x64"
)

declare -A TRIPLE_TO_EXT=(
  ["x86_64-unknown-linux-gnu"]="so"
  ["aarch64-unknown-linux-gnu"]="so"
  ["x86_64-apple-darwin"]="dylib"
  ["aarch64-apple-darwin"]="dylib"
  ["x86_64-pc-windows-gnu"]="dll"
)

# Cargo's own output prefix for the platform. The staged copy is always
# `libflz_core_ffi.<ext>`; only the PHP package keeps the platform spelling.
declare -A TRIPLE_TO_PREFIX=(
  ["x86_64-unknown-linux-gnu"]="lib"
  ["aarch64-unknown-linux-gnu"]="lib"
  ["x86_64-apple-darwin"]="lib"
  ["aarch64-apple-darwin"]="lib"
  ["x86_64-pc-windows-gnu"]=""
)

# Cargo's executable suffix for the platform; empty everywhere but Windows.
declare -A TRIPLE_TO_BIN_EXT=(
  ["x86_64-unknown-linux-gnu"]=""
  ["aarch64-unknown-linux-gnu"]=""
  ["x86_64-apple-darwin"]=""
  ["aarch64-apple-darwin"]=""
  ["x86_64-pc-windows-gnu"]=".exe"
)

# The published name of one triple's asset on a GitHub Release.
release_asset_name() {
  local dir="$1" ext="$2"
  echo "libflz_core_ffi-${dir}.${ext}"
}

# The published name of one triple's fake controller. An SDK repository's CI
# fetches it by this name the way the Java build fetches a missing library.
release_binary_name() {
  local dir="$1" bin_ext="$2"
  echo "flz-fake-controller-${dir}${bin_ext}"
}
