# frozen_string_literal: true

# Build the native veneer for the source gem.
#
# A precompiled platform gem ships `lib/flezha/flz_core_ffi.<dlext>` and needs
# no toolchain at all — that is the primary distribution, and this file is the
# fallback for platforms it does not cover. The fallback needs a Rust
# toolchain, and says so rather than failing inside cargo.

require "mkmf"
require "fileutils"

ext_dir = __dir__
gem_root = File.expand_path("../..", ext_dir)
dlext = RbConfig::CONFIG["DLEXT"]
prebuilt = File.join(gem_root, "lib", "flezha", "flz_core_ffi.#{dlext}")

if File.exist?(prebuilt)
  # A platform gem, or a repo checkout whose build script already installed the
  # library. Nothing to compile; write a Makefile that does nothing so
  # RubyGems' extension step succeeds.
  File.write(File.join(ext_dir, "Makefile"), <<~MAKE)
    all:
    \t@true
    install:
    \t@true
    clean:
    \t@true
  MAKE
  exit 0
end

workspace = File.expand_path("../..", gem_root)
manifest = File.join(workspace, "flz_core_ffi", "Cargo.toml")

unless File.exist?(manifest)
  abort <<~MESSAGE
    flezha: no precompiled native library for #{RUBY_PLATFORM}, and no Rust workspace
    to build one from. Install a precompiled platform gem, or build from a checkout
    of the flezha repository, where `scripts/build-ruby-sdk.sh` produces it.
  MESSAGE
end

abort "flezha: building the source gem needs a Rust toolchain (`cargo` was not found)" unless
  find_executable("cargo")

profile = ENV.fetch("FLEZHA_RUST_PROFILE", "release")
args = ["cargo", "build", "--manifest-path=#{manifest}", "--features", "ruby"]
args << "--release" if profile == "release"

abort "flezha: cargo failed to build flz_core_ffi" unless system(*args)

built = File.join(workspace, "target", profile, "libflz_core_ffi.#{RbConfig::CONFIG['SOEXT'] || 'so'}")
FileUtils.mkdir_p(File.dirname(prebuilt))
FileUtils.cp(built, prebuilt)

File.write(File.join(ext_dir, "Makefile"), <<~MAKE)
  all:
  \t@true
  install:
  \t@true
  clean:
  \t@true
MAKE
