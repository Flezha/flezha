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
```

`format-check.sh` exits **1** when something is unformatted and **2** when a
formatter is not installed on your box; the summary names which, and every
other language is still reported either way. Exit 2 means CI has yet to say
anything about that language — it is not a pass.

Two repo-wide rules that are easy to miss:

- **One change, every SDK.** Any surface all SDKs carry — declarations, wire
  protocol, defaults, bench and e2e fixtures — changes in Rust, Java and Elixir
  in the same change, or the change names what it defers. Normative text:
  `docs/architecture/runner.md`.
- **No build artifact is committed.** Native libraries and fat jars are build
  output; `scripts/check-build-artifacts.sh` fails a pull request that tracks
  one. Build the host triple once with `scripts/build-native.sh` and every Java
  test path works offline — see README "Building".

Keep a change to one concern, and read the matching `docs/dev_notes/` entry
before touching an area listed in `CLAUDE.md`.

## Security issues

Do not open a public issue — see [SECURITY.md](SECURITY.md).
