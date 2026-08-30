# Open-source licensing foundation

## Decisions

- **Controller → MPL-2.0, SDK side → Apache-2.0.** Copyright holder `Anuar
  Alfetahe`. The SDK side is `sdk/`, `flz_core/`, `flz_core_ffi/`,
  `flz_transport/`, `flezha-macros/`, `lib/flezha/sdk/`, `protobufs/`.
- **Contributors sign the Individual CLA** (`CLA.md`) through cla-assistant; it
  is what keeps the right to relicense or dual-license later.
- **Nothing is published.** No crates.io, hex, Maven Central or image push
  happened with this change; metadata is in place so publishing later is a
  decision, not a scramble.

## Load-bearing gotchas

- **The Elixir SDK split exists only in `REUSE.toml`.** `lib/flezha/sdk/*.ex`
  compiles into the same `:flezha` app and release as the MPL controller, so no
  directory LICENSE can express it and the repo has no per-file headers. The
  second `[[annotations]]` table (Apache) wins over the first (`**`, MPL) for
  its paths; `reuse spdx` proves the precedence, `reuse lint` in the `format`
  CI job keeps it.
- **`flz_transport` must stay Apache-2.0.** It is a direct dependency of
  `flz_core`, `flz_core_ffi`, `flezha_runner` *and* the controller NIF
  `flz_nif`. An MPL crate may depend on an Apache one; the reverse would put
  copyleft on the SDK link path.
- **`process_hub` must never regress to GPL.** It was GPL-3.0 until 2026-08-23
  and is relicensed to Apache-2.0; GPL on a runtime dependency makes MPL on the
  controller indefensible. Since `adopt-process-hub-hex-release` the dependency
  is a hex release, so the licence is in the package metadata — `mix hex.info
  process_hub` states it — and is worth re-reading after any bump.
- **The shaded jars carry notices through the shade plugin, never through
  upstream `META-INF` files.** Same-named `META-INF/LICENSE(.txt)` and
  `NOTICE(.txt)` from Jackson, Objenesis and SLF4J overwrite each other in a
  fat jar (only the first survived before); they are now filtered and their
  contents reproduced in `THIRD_PARTY_NOTICES.md`, which an
  `IncludeResourceTransformer` adds next to Flezha's own `META-INF/LICENSE`
  and `NOTICE`. Uniquely named upstream files (`FastDoubleParser-*`,
  `thirdparty-LICENSE`) ride through untouched. `ServicesResourceTransformer`
  merges `META-INF/services/*` — before, only one entry survived.
- **`THIRD_PARTY_NOTICES.md` is generated.** `cargo about generate about.hbs`
  runs at the end of `scripts/build-native.sh` (the script that already owns
  "the binaries changed"); the `format` CI job regenerates and diffs it. The
  Java section of `about.hbs` is static, maintained against the versions
  pinned in `sdk/java/pom.xml`. `accepted` order in `about.toml` decides which
  side of an `OR` is attributed (MIT first, so each crate's own copyright line
  is reproduced).
- **Legal files ride the release through `copy_release_extras/1`** in
  `mix.exs` (the former `copy_cli_commands/1`), so the controller image gets
  them from the release copy, not from a second `COPY`.
- **Publish guards:** `flz_nif`, `flz` (cli) and `test/apps/rust_runner` carry
  `publish = false`; the SDK crates inherit `license`/`repository` from
  `[workspace.package]`.

## Test map

- `reuse lint` (CI, clean checkout) / `git ls-files | reuse lint-file`
  (`scripts/format-check.sh` — a full lint walks the tree and chokes on the stale
  test sockets under `app_data/`, which git never lists); `reuse spdx` proves the
  precedence (`lib/flezha/sdk/ctx.ex` → Apache-2.0, `lib/flezha/operative/ingest.ex`
  → MPL-2.0).
- `cargo metadata --no-deps` — every crate has a license, the three
  non-SDK crates are unpublishable.
- `unzip -l sdk/java/flezha-sdk/dist/flezha-sdk.jar | grep META-INF` — exactly
  one `LICENSE`, one `NOTICE`, `THIRD_PARTY_NOTICES.md`, merged services.
- `mix release` — `LICENSE`, `LICENSES/`, `NOTICE`, `THIRD_PARTY_NOTICES.md`
  in the release root.
