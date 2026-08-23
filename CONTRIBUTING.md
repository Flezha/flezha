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
scripts/format-check.sh        # cargo fmt, mix format, spotless, reuse lint, third-party notices
scripts/check-sdk-boundary.sh  # the Elixir SDK must not reach into controller internals
scripts/check-kind-naming.sh   # operative kinds are `stream` and `workflow`
```

Two repo-wide rules that are easy to miss:

- **One change, every SDK.** Any surface all SDKs carry — declarations, wire
  protocol, defaults, bench and e2e fixtures — changes in Rust, Java and Elixir
  in the same change, or the change names what it defers. Normative text:
  `docs/architecture/runner.md`.
- **Committed distribution artifacts.** Edits under `sdk/java/`,
  `flz_transport/` or `test/apps/java_runner/` require refreshed jars in the
  same commit (`scripts/build-dist.sh`); the pre-commit hook from
  `scripts/install-hooks.sh` enforces it.

Keep a change to one concern, and read the matching `docs/dev_notes/` entry
before touching an area listed in `CLAUDE.md`.

## Security issues

Do not open a public issue — see [SECURITY.md](SECURITY.md).
