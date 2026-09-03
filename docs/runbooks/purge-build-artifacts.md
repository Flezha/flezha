# Runbook — purging build artefacts from git history

One-time operation. It rewrites every commit hash and force-pushes every branch
and tag. Run it only after the flow is stopped: an artefact still being
committed grows back, so `scripts/check-build-artifacts.sh tracked` must be
green first.

## What it removes

Eight paths, all build output. Their sources are untouched.

    sdk/java/flezha-sdk-native/src/main/resources/native/
    sdk/java/flezha-sdk/dist/
    test/apps/java_runner/dist/
    test/apps/java_runner/target/
    test/apps/java_executor/dist/
    test/apps/java_executor/target/
    test/apps/java_compute_engine/dist/
    test/apps/java_compute_engine/target/
    priv/native/

`test/apps/java_executor/` and `test/apps/java_compute_engine/` are retired
directories, absent from the tree. Do **not** purge them whole: they hold
188.6 MB of build output but also 0.2 MB of source across 93 blobs — `pom.xml`,
`README.md`, and the `.java` files that are the only record of how those test
apps were written. Purging `dist/` and `target/` under them takes the bytes and
keeps the history. Every binary they ever held is under those two directories;
nothing else in them exceeds a few KB.

`priv/native/` holds 79 MB across 35 committed NIFs, now gitignored. It is not
optional: no single blob there exceeds 5 MB, so a "no blob over 5 MB" check
passes while the bytes stay.

## Before

Pick a scratch directory for the mirror and the working copy. It has to be
outside this checkout, and on a filesystem with room for twice the repository —
`filter-repo` writes a rewritten copy before replacing the old. `/tmp` qualifies
only where it is not a small tmpfs. `target/debug/incremental` is usually the
largest reclaimable thing on the box if room has to be made, and is pure cache.

    WORK="${TMPDIR:-/tmp}/flezha-purge-$(date +%F)"; mkdir -p "$WORK"
    scripts/check-build-artifacts.sh tracked     # must exit 0
    git push origin <branch>                     # land all work first
    git push origin backup/dev-pre-unbundle-<date> refs/tags/pre-unbundle-<date>
    git clone --mirror . "$WORK/flezha-mirror-pre-unbundle.git"

The mirror is the real insurance: it carries every ref and every object,
including branches that exist nowhere else. Keep it until a fresh clone has
built and tested green.

## The rewrite — on a working copy, never on the live repo

    git clone --mirror "$WORK/flezha-mirror-pre-unbundle.git" "$WORK/flezha-purge-work.git"
    cd "$WORK/flezha-purge-work.git"
    git filter-repo --force --invert-paths \
      --path sdk/java/flezha-sdk-native/src/main/resources/native/ \
      --path sdk/java/flezha-sdk/dist/ \
      --path test/apps/java_runner/dist/ \
      --path test/apps/java_runner/target/ \
      --path test/apps/java_executor/dist/ \
      --path test/apps/java_executor/target/ \
      --path test/apps/java_compute_engine/dist/ \
      --path test/apps/java_compute_engine/target/ \
      --path priv/native/

## Verify by measurement, never by eye

`fix.sh` on 2026-08-29 "succeeded" and removed zero bytes, because nothing
checked the result. These four numbers are the acceptance:

| check | expected |
|---|---|
| tip tree hash, before vs after | **identical** — proves no source was collateral damage |
| blobs over 5 MB, all refs | **0** |
| fresh clone `.git` | **under 150 MB** |
| heads and tags | **unchanged in count** |

Measured on 2026-08-30: 959 MB → 14 MB fresh clone, 2211 MiB → 158 MiB of blob
bytes, 163 → 0 blobs over 5 MB, tip tree `c4d492bb` identical, 9 heads and 12
tags preserved.

When counting blobs, `git cat-file --batch-check` **must** include `%(rest)` in
its format or it errors on every line `git rev-list --objects` gives it a path
for, and reports zero. That mistake has produced a false "all clean" twice.

## Commits that disappear

`filter-repo` prunes commits left empty. Four went on `dev` — `compiled nifs.`,
`Nif shared objects.`, `Refresh Java executor test bytecode`,
`chore(java_executor): refresh tracked test-app build artifacts` — all pure
artefact refreshes. Confirm any dropped commit touched **only** purged paths
before accepting the rewrite.

`filter-repo` also rewrites short hashes **inside commit messages**, so a commit
whose subject cites another (`Revert 5e22d77: …`) changes subject without being
dropped. Search by content, not by the quoted hash, before concluding a commit
is gone.

## Citations in tracked files (this is not automatic)

`filter-repo` rewrites hashes in commit messages only. Tracked files that cite
commits must be repointed from `filter-repo/commit-map`:

    .git-blame-ignore-revs

`.git-blame-ignore-revs` and `baseline.json` are **not** markdown and are the two
most often missed. Both are functional, not prose: git blame skips those revs,
and `refactor_metrics.py --check` reads `baseline.json`'s `head`. A scan limited
to `*.md` leaves both broken.

Verify afterwards that every commit-shaped citation resolves, and that
`baseline.json` still parses.

## Publish

    git push --mirror origin        # from the purged working copy

Then re-clone locally and prove the result:

    mix test
    cargo test --workspace --features flezha_runner/bench-internals

## After

Every existing clone is now invalid and must be re-cloned — no `git pull`
recovers from a rewrite. Sibling projects that record a flezha commit for
provenance need re-syncing: `flezha_market` holds <!-- docs-citation-record:begin -->`FLEZHA_SHA`<!-- docs-citation-record:end --> in
<!-- docs-citation-record:begin -->`kubernetes/lib/common.sh`, `docker/dev/sync-flezha-local.sh`<!-- docs-citation-record:end --> and
<!-- docs-citation-record:begin -->`docker/dev/run-quality.sh`<!-- docs-citation-record:end -->. `.flezha-local` is an rsync copy rather than a
clone, so nothing breaks functionally, but the recorded SHAs stop resolving.
Pre-rewrite hashes quoted in that repo's archived proposals cannot be repointed
and stay dangling.

`repo-size` in `.github/workflows/naming.yml` is blocking, and has been since
this purge landed. It is the ceiling that keeps the reclaimed bytes reclaimed.

## Two traps the first real run found

**Cross-check all five triples locally before pushing a release tag.** CI sets
`RUSTFLAGS: -D warnings`, so an import that is live on Unix and dead on Windows
is an error there and only a warning here. One CI round trip costs ~6 minutes;
all five locally cost minutes once:

    RUSTFLAGS="-D warnings" cargo zigbuild -p flz_core_ffi --features jni --target <triple>

Fixing the first error CI reports and pushing again, without scanning for the
rest, turns one defect into one round trip each.

**A private repository needs `gh`, not curl.** `/releases/download/<tag>/<asset>`
is public-only and answers 404 for a private repo whether or not a token is
presented — the endpoint takes no auth. `--fetch-missing` prefers `gh` for that
reason and keeps curl as the public fallback.
