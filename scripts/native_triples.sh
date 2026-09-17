# The five supported target triples, and how each one is spelled: as a staged
# directory, as a library extension and prefix, and as a published release
# asset. Sourced, never executed.
#
# `scripts/build-dist.sh` publishes assets under these names. An SDK developed
# in a repository of its own fetches them from there, where no script of this
# tree is sourced, so each such fetch spells the asset name and the directory
# column again. The same table also decides the resource path the Java runtime loads
# from — see sdk/java/flezha-sdk-native/.../NativeLibraryLoader.java, whose
# SUPPORTED set is the directory column.

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

# The platform a precompiled Ruby gem is published for, as RubyGems names it.
# The Windows gem targets the UCRT toolchain the Ruby installers ship, not the
# mingw one the library is cross-built with.
declare -A TRIPLE_TO_GEM_PLATFORM=(
  ["x86_64-unknown-linux-gnu"]="x86_64-linux"
  ["aarch64-unknown-linux-gnu"]="aarch64-linux"
  ["x86_64-apple-darwin"]="x86_64-darwin"
  ["aarch64-apple-darwin"]="arm64-darwin"
  ["x86_64-pc-windows-gnu"]="x64-mingw-ucrt"
)

# The published name of one triple's asset on a GitHub Release.
release_asset_name() {
  local dir="$1" ext="$2"
  echo "libflz_core_ffi-${dir}.${ext}"
}

# The published name of one triple's fake controller. An SDK repository's CI
# fetches it by this name.
release_binary_name() {
  local dir="$1" bin_ext="$2"
  echo "flz-fake-controller-${dir}${bin_ext}"
}

# The Elixir SDK's socket client as `rustler_precompiled` names it, without the
# `.tar.gz` it is published in: the NIF version is the oldest one the package
# declares, and the extension is the one the BEAM loads a NIF by.
elixir_nif_name() {
  local triple="$1" version="$2" nif_version="2.16"
  case "${triple}" in
    *windows*) echo "flz_sdk_nif-v${version}-nif-${nif_version}-${triple}.dll" ;;
    *)         echo "libflz_sdk_nif-v${version}-nif-${nif_version}-${triple}.so" ;;
  esac
}
