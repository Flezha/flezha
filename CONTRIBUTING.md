# Contributing to Flezha

## Contributor License Agreement

Every contribution requires a signed [Individual CLA](CLA.md). The
cla-assistant bot asks for the signature on your first pull request; signing
once covers all later contributions. Your contribution is licensed under the
license of the path it lands in — MPL-2.0 or Apache-2.0 per
[`REUSE.toml`](REUSE.toml) — and the CLA is what lets the project keep that
split coherent over time.

## Before opening a pull request

Run the same gates CI runs:

```bash
scripts/format-check.sh        # every formatter, reuse lint, third-party notices
scripts/check-sdk-boundary.sh  # the Elixir SDK must not reach into controller internals
scripts/check-kind-naming.sh   # operative kinds are `stream` and `workflow`
scripts/check-mimic-global.sh  # a test must release Mimic's global mode
scripts/check-layer-direction.sh  # a lower directory must not name a higher one
```

`check-layer-direction.sh` reads `lib/flezha/` against the controller's
dependency order: a file in one directory may not name a module whose own file
lives in a higher one, and every module should live in some directory. A
module's layer is where its FILE sits, not what it is called, so moving a file
into the directory that owns its feature is what closes an entry.
`scripts/layer-direction-baseline.txt` records what the tree held when the gate
landed — the gate fails on anything outside that list and on any entry the tree
no longer holds, so the list may only shrink. A directory the gate's layer
table does not rank makes it exit 2: rank the new directory in the script
before adding code to it.

`format-check.sh` reports every language it can and ends on one of three
non-zero statuses: **1** when something is unformatted, **2** when a formatter
is not installed on your box, and **3** when a formatter ran and did not finish
— a missing runtime extension, an input it cannot parse, a toolchain it cannot
reach. The summary names which checks landed where, and every other language is
still reported whichever it is. Only **1** says anything about your source: on
**2** and **3** that language has been checked by nothing, so neither is a pass.

Two repo-wide rules that are easy to miss:

- **One change, every SDK.** Any surface all SDKs carry — declarations, wire
  protocol, defaults, bench and e2e fixtures — changes in Rust, Java and Elixir
  in the same change, or the change names what it defers. Normative text:
  `docs/architecture/runner.md`.
- **No build artifact is committed.** Native libraries and fat jars are build
  output; `scripts/check-build-artifacts.sh` fails a pull request that tracks
  one. Build the host triple once with `scripts/build-native.sh` and every Java
  test path works offline — see README "Building".

Keep a change to one concern.

## Security issues

Do not open a public issue — see [SECURITY.md](SECURITY.md).
