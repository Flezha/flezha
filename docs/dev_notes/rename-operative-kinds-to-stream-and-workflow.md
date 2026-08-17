# Operative kinds are `stream` and `workflow` (`rename-operative-kinds-to-stream-and-workflow`)

<!-- kind-naming-record:begin -->
- **`:unbounded` → `:stream`, `:bounded` → `:workflow`**, everywhere a developer
  reads: kind atoms and wire literals, SDK types in both SDKs, protobuf message
  names (tags 22/23/34/35 unchanged), CLI `--kind`, env vars, module/file/spec
  directory names, comments and docs. BREAKING, no shim. Behaviour, durability
  and every wire tag number are untouched — only names.
- **GOTCHA — three senses of the word, and only one renamed.** *Kind* (renamed),
  *capacity* (`BoundedMailboxSource`, `:flezha_bounded_mailbox`, "backpressure
  and bounded mailboxes" — untouched) and *plain English* ("bounded by",
  "a bounded (1024) FIFO", "unbounded growth" — untouched). A repo-wide
  `s/bounded/workflow/` corrupts ~65 English uses and the mailbox subsystem, and
  it still **compiles** — that is why `scripts/kind-naming-allowlist.txt` was
  built BEFORE any edit and is the input to both the sweep and the gate.
- **GOTCHA — the binding family shares no substring.** `bound`, `unbound`,
  `bind`, `rebind`, `misbound`, `inbound`, `bound_runner`,
  `require_bound_principal` mean *attached to a runner or namespace*;
  `unbound` ("not placed") is unrelated to `unbounded`. Widening any pattern
  from `bounded` to `bound` corrupts ~210 sites.
- **GOTCHA — `crossbeam::channel::unbounded` / `bounded` are third-party
  capacity APIs**, not kinds. They are allowlisted; renaming them is the one
  corruption that *does* fail the build (it did, once, in `flz_transport`).
- **GOTCHA — the English sense also appears as IDENTIFIERS, and those renames
  compile.** `Convergence.SafeCall.bounded/3` is a *time-limited* `GenServer.
  call`, not a kind; the sweep turned it into `SafeCall.workflow/3` across nine
  files and nothing failed. Same class of silent damage: "bounded-retry
  governor" (Overwatch's start ladder), "bounded ack-wait", "bounded outbound
  queue" (Kafka), "**Bounded capacity**" (`mailbox.rs`), "bounded park"
  (`routing.rs`), "bounded quota", "bounded margin", "bounded primitives". A
  phrase allowlist is NOT enough — after the sweep, audit by grouping every
  renamed occurrence by its neighbouring word and reading the groups whose
  neighbour is not an operative noun. That audit is what found all of them.
- **`scripts/check-kind-naming.sh` is the permanent gate** (CI workflow
  `naming.yml`, path-unfiltered, toolchain-free). It matches any identifier
  *containing* `bounded` — not just the bare word, so `BoundedOperative` and
  `spawn_bounded` are caught — masks each allowlisted phrase out of the line,
  and fails on the residue. Scope is `git ls-files`, so `openspec/changes/`
  (which holds the archive), `target/` and `_build/` are excluded by being
  ignored; `dist/` and binaries are excluded explicitly.
- **The old words are KEPT where they are true**, as adjectives for the run:
  a workflow operative runs a *bounded* iteration to a terminal state; a stream
  operative folds an *unbounded* input forever. `docs/glossary.md` carries that,
  the `PerInput` caveat (its input is unbounded; only its iteration is bounded —
  the fact that makes the old name false rather than merely unfamiliar) and the
  one-sentence rationale, so the question is settled rather than reopened.
- **GOTCHA — the archive keeps the old ids.** `openspec/changes/archive/**` is a
  historical record and is out of scope, so the change directories are still
  date-prefixed `bounded-loop-rewind`, `bounded-op-journal`,
  `bounded-segment-retry`, … even
  though the section headings here and the live `openspec/specs/` capabilities
  now say `workflow-*`. Grep the archive with the old name.
- **UPGRADE — `flz controller stop && flz reset --yes` is mandatory.** The kind
  atom is persisted in the ProcessHub DETS cspec AND in `ClassRegistry`'s
  on-disk sidecar; nothing reads the retired atoms. Controller and every runner
  redeploy together. `flezha_market` ConfigMaps referencing `FLEZHA_BOUNDED_*`
  need the rename in that repo before its next deploy.
- **GOTCHA — trybuild `.stderr` fixtures moved for a non-obvious reason.** Five
  UI cases now print `flezha_runner::Stream` where they printed `Stream`;
  renaming the sibling module `bounded` → `workflow` changed which path rustc
  picks as shortest-visible. Semantically identical, blessed with
  `TRYBUILD=overwrite`.
- **GOTCHA — `EntityIdTest` pins byte OFFSETS, not just content.** Its fixture
  name shortened by 3 bytes (`MarketUnboundedOperative` → `MarketStreamOperative`)
  and every hard-coded index in the framing assertion had to move with it.
- **Rebuilt artifacts**: `scripts/build-dist.sh` after `mvn clean` in
  `sdk/java` — the committed `sdk/java/flezha-sdk/dist/`,
  `test/apps/java_runner/dist/` and the five `flezha-sdk-native` binaries embed
  the api classes, and the fat jar shades stale `target/classes` first.
<!-- kind-naming-record:end -->

