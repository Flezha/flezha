# Docker BUILD

Pinned toolchain image used to produce the distribution artifacts that
ride along in the repo (so CI never has to rebuild them):

- `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/libflz_transport.{so,dylib,dll}`
  — flz_jni cdylib for **all five** target platforms (linux-x64,
  linux-arm64, macos-x64, macos-arm64, windows-x64).
- `sdk/java/flezha-sdk/dist/flezha-sdk.jar` — SDK fat jar.
- `test/apps/java_executor/dist/java_executor.jar` —
  test executor fat jar.

This image is **only for builds**, not for running the controller. For
runtime/dev see [`../dev/README.md`](../dev/README.md); for production
see [`../prod/README.md`](../prod/README.md) (image: `Dockerfile.controller`).

## Why a separate image?

The controller images don't need cross-compile toolchains, Maven, or the JDK
in `PATH`. The build image bundles all of it (Rust + 5 cross targets,
`cargo-zigbuild`, `zig` via the `ziglang` Python wheel, Maven 3.9, JDK
21) so the build is hermetic — same input, same output, regardless of
what's on your host.

## Usage

From the project root, the one-shot wrapper is `scripts/build-dist.sh`:

```bash
# Build everything inside the pinned container.
# First run pulls the base image and builds the layer (~5 min); cached after.
scripts/build-dist.sh --docker

# Force rebuild the image (after editing Dockerfile.dist):
FLEZHA_REBUILD_IMAGE=1 scripts/build-dist.sh --docker

# Same script on the host without docker (auto-installs cargo-zigbuild + zig):
scripts/build-dist.sh
```

The `--docker` mode bind-mounts the repo at `/flezha` and uses a named
volume `flezha-dist-build-cache` for `~/.m2`, `CARGO_TARGET_DIR`, and the
container's `$HOME`, so subsequent runs are fast. Output files land in
the bind-mounted repo with **your host UID** (`docker run --user
$(id -u):$(id -g)`), so no `sudo chown` is needed afterward.

## Image contents

`Dockerfile.dist` installs:

| Tool                    | Source                                    | Where it lands       |
|-------------------------|-------------------------------------------|----------------------|
| Eclipse Temurin JDK 21  | `eclipse-temurin:21-jdk-jammy` base       | `/opt/java/openjdk/` |
| Maven 3.9.9             | Apache mirror tarball                     | `/opt/apache-maven/` |
| Rust stable             | rustup, no `--modify-path`                | `/usr/local/cargo/`  |
| 5 rustup targets        | `rustup target add ...`                   | `/usr/local/rustup/` |
| `cargo-zigbuild`        | `cargo install --locked`                  | `/usr/local/cargo/bin/` |
| `zig`                   | `python3 -m venv` + `pip install ziglang` | `/opt/zig-venv/`     |

Everything is world-rwx so the container is safe to run with `--user
<any-uid>`.

## Updating the image

Edit `Dockerfile.dist`, then:

```bash
FLEZHA_REBUILD_IMAGE=1 scripts/build-dist.sh --docker
```

If you bump pinned versions (Maven, JDK base image, Rust toolchain),
also refresh the artifacts on the same commit so reproducibility is
checked in alongside the toolchain change.
