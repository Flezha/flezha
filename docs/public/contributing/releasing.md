# Releasing

What a release publishes, and how each part is built. None of it is tracked in git. Working on Flezha needs none of this page:
the tests read only the native library for your own platform, which
[Setting up a checkout](setting_up.md#the-native-library-for-java) builds.

## The artifacts

| Artifact | Where it is built to |
|---|---|
| The controller archive for linux-x86_64, macos-arm64 and macos-x86_64 | `_build/.dist/controller/`, by `make release-archive` on each platform |
| `libflz_core_ffi` for five platforms: linux-x64, linux-arm64, macos-x64, macos-arm64, windows-x64 | `sdk/java/flezha-sdk-native/src/main/resources/native/<platform>/` |
| The Java SDK jar, with the five native libraries inside | `sdk/java/flezha-sdk/dist/flezha-sdk.jar` |
| The Java test runner jar | `test/apps/java_runner/dist/java_runner.jar` |

## On a version tag

Pushing a `v*` tag, or creating a GitHub release with a new one, runs
`.github/workflows/release.yml`:

1. The controller archive is built on a Linux, an Apple Silicon and an Intel
   Mac runner, and each runs `scripts/release-archive.sh smoke` on its own
   runner: a controller starts from the unpacked archive, a runner connects and
   is given a relay, and the controller stops.
2. On Linux, the publication readiness gate runs
   (`python3 scripts/run_gates.py --run @release`), then the five native
   libraries, the SDK packages and both jars are built, and everything is
   staged with one `SHA256SUMS.txt`.
3. The two macOS libraries are loaded once on a Mac of their own chip.
4. Only then is everything attached to the release. A failure anywhere attaches
   nothing, and a tag with a `-` in it, such as `v0.1.0-rc.1`, makes a
   pre-release.

[The publication readiness checklist](https://github.com/flezha/flezha/blob/main/docs/internal/publishing/readiness.md)
lists what has to be settled before a release, and
[Maturity](../publishing/maturity.md) what each part of Flezha promises.

## Building them yourself

```sh
$ scripts/build-dist.sh             # on your machine; installs zig and cargo-zigbuild when missing
$ scripts/build-dist.sh --docker    # in the pinned build image; needs only Docker
```

Or fetch the published native libraries instead of cross-building them:

```sh
$ mvn -P native-build -f sdk/java/pom.xml package
```

The `native-build` profile downloads each platform missing from your tree from
the release that matches the pinned version, checks it against the manifest, and
leaves the ones you built alone. It is the only build path that uses the
network, and no test uses it.

### The build image

`deploy/docker/build/Dockerfile.dist` holds every tool the cross-build needs, so the
result does not depend on what your machine has installed:

| Tool | Where it lands |
|---|---|
| Eclipse Temurin JDK 21, the base image | `/opt/java/openjdk/` |
| Maven 3.9.9 | `/opt/apache-maven/` |
| Rust stable, with the five targets | `/usr/local/cargo/`, `/usr/local/rustup/` |
| `cargo-zigbuild` | `/usr/local/cargo/bin/` |
| `zig`, from the `ziglang` Python wheel | `/opt/zig-venv/` |

`--docker` mounts the repository at `/flezha` and runs as your own user, so the
output belongs to you. The Maven repository, the Cargo target directory and the
container's home directory live in the named volume `flezha-dist-build-cache`,
which keeps later runs fast. The first run builds the image. After editing
`Dockerfile.dist`, rebuild it:

```sh
$ FLEZHA_REBUILD_IMAGE=1 scripts/build-dist.sh --docker
```

A change to a pinned version in the image changes the next release built from
it, so the release notes say so.

## The Docker images

The controller image is built per architecture and published as one
multi-architecture image:
[the production Docker README](https://github.com/flezha/flezha/blob/main/deploy/docker/prod/README.md)
has the steps, the image size budget and the platforms. The development images
are built with the `make` targets in
[Setting up a checkout](setting_up.md#the-images), and published the same way.
