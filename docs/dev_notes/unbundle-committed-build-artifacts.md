# Unbundling the committed build artefacts

Seven build artefacts left git and 947 MB left the history. What follows is what
the work actually cost and what it found — not what the proposal expected.

## Measured, before and after

| | before | after |
|---|---|---|
| fresh clone `.git` | 961 MB | **15 MB** |
| blob bytes across all refs | 2211 MiB | 159 MiB |
| blobs over 5 MB | 163 | **0** |
| tip tree | `585a02b5` | `585a02b5` — identical |
| heads / tags | 9 / 12 | 9 / 12 |
| commits on `dev` | 1148 | 1144 |

Every one of the 163 blobs over 5 MB the history ever held was in one of the
purged path groups. Nothing else in this repository has ever been big.

## The purge is narrower than the proposal asked for

`tasks.md` said to purge `test/apps/java_executor/` and
`test/apps/java_compute_engine/` whole. Measured, those two hold **188.6 MB of
build output and 0.2 MB of source across 93 blobs** — `pom.xml`, `README.md`,
and the `.java` files that are the only record of how those retired test apps
were written. Purging them whole would have destroyed that history to save
0.2 MB. Every binary they ever held is under `dist/` or `target/`, so the purge
names those and keeps the source.

`priv/native/` was **not** in the task list and had to be added: 79 MB across 35
committed NIFs. No single blob there exceeds 5 MB, so the "no blob over 5 MB"
acceptance check would have passed while the bytes stayed.

## Four commits disappear, and that is correct

`filter-repo` prunes commits left empty: `compiled nifs.`, `Nif shared
objects.`, `Refresh Java executor test bytecode`, `chore(java_executor): refresh
tracked test-app build artifacts`. All four touched only purged paths.

A fifth *looked* dropped and was not. `filter-repo` rewrites short hashes inside
commit messages, so `Revert 5e22d77: …` became `Revert aa918d0: …` and a search
for the quoted hash found nothing. Search by content before concluding a commit
is gone.

## What the first real release trial found

Restoring the five-triple cross-build on a tag — deleted from CI in April 2026 —
failed on its first run, and the cause had been sitting in the tree for months:

    error: unused import: `socket_dir`  --> flz_core/src/control_unit.rs:19

`refactor(1b)` moved that import in without carrying across the `#[cfg(unix)]`
that `a9e27c6b` had put on `try_open_uds`, its only caller. It stayed invisible
because CI had stopped cross-compiling, the committed artefacts were always
built on Linux where the import is live, and **locally an unused import is a
warning, not an error** — only CI's `RUSTFLAGS: -D warnings` makes it fatal.
`build-dist.sh` had been building a Windows binary and printing that warning
into a log nobody read.

The second trial then found that `--fetch-missing` could not work at all here:
`/releases/download/<tag>/<asset>` is public-only and answers 404 for a private
repository **whether or not a token is presented**, because that endpoint takes
no auth. `gh` reaches the same asset through the API. The local HTTP-server
drive could not have caught this — it was serving without auth in the first
place.

Both are the argument for 6.2/6.3 being real tasks rather than "proved
equivalently offline".

## Three measurement traps, each of which produced a false clean result

1. **`git cat-file --batch-check` without `%(rest)`** errors on every line
   `git rev-list --objects` hands it a path for, and reports **zero blobs**.
   This produced a confident "0 blobs over 5 MB" on an untouched repository.
   Three times in one session.
2. **`| tail` on a suite run** captures `Failed: 1 test` and none of the failure
   block, which prints long before the summary. Two consecutive sessions lost
   the same failure to it. Redirect to a file and grep.
3. **Fixing the first error CI reports and pushing again.** One CI round trip is
   ~6 minutes. All five triples locally under `-D warnings` cost minutes once:
   `RUSTFLAGS="-D warnings" cargo zigbuild -p flz_core_ffi --features jni --target <t>`.

## Backups, and why the remote one had to go

A backup branch on the remote keeps the old history reachable, and `git clone`
fetches every ref — so while `backup/dev-pre-unbundle-*` pointed at the
pre-rewrite tip, a fresh clone pulled the old 961 MB back down and the purge
bought nothing. Deleting it would have left the only pre-rewrite copy on one
laptop.

Resolved with a separate private repository,
`alfetahe/flezha-archive-pre-unbundle`, holding the frozen mirror (28 refs,
including `RESCUE-area3-45cbd1db` and `spike/1s-dispatcher`). The main
repository is then genuinely purged and the history is still off-machine.

## Replacing the local clone

Do not re-clone: `target/` alone is 14 GB of build cache, plus `_build`, `deps`
and the seven artefacts the Java suites need on disk. Swap `.git` instead —
`mv .git <aside>`, `git init`, add the remote, `git checkout -f -B dev
origin/dev`. The tip tree is identical, so tracked files do not move.

`.git/info/exclude` does **not** travel with a clone and is lost in the swap.
Copy it across, or three local-only paths reappear as untracked.

## `flezha_market`

Its `FLEZHA_SHA` provenance record is repointed through the commit map
(`f02a4cf7` → `73de7482`, the same commit). Its `.flezha-local` source snapshot
was already two weeks stale — it predates area 3 — and re-syncing it changes
what that project builds against, so it is left for its owner to decide.
Pre-rewrite hashes quoted in its archived proposals cannot be repointed.
