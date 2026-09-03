# Open work — everything unsolved, in plain language

**What this is**: every open item in one table, in plain language, without jargon. Two sentences per
item and one minimal example. **The supervisor keeps this current** and reports it to the owner **once
an hour**.

**Last updated**: 2026-09-03 02:05 · **Last reported**: 2026-09-03 02:05

**RULE FOR THE NEXT SESSION**: take the **oldest and most serious** entry, never the successor of the one
you just fixed. Session 10 lost its last two hours by taking each freshly filed entry immediately — the
list went 8 → 9. **KI-55 and KI-57 are the two serious ones and nobody touched either.**

---

## 1. Defects that produce a wrong result

| # | Problem | Example | State |
|---|---|---|---|
| KI-55 | A transform placed between two procedures runs **twice** in some SDKs. Nothing reports it — a wrong value simply moves on. | The rule "prefix `M:`" yields `M:M:mid:v` where it should yield `M:mid:v`. | **Open. Serious** |
| KI-57 | Under a full run, socket paths grow past the operating system's limit. The runner cannot connect at all, and everything that needed it is silently left undone. | `/tmp/flezha_.../stream_...sock` exceeds 107 characters and the connection never forms. | **Open. Serious** |
| KI-154 | `flz operative info` reports a PARKED operative as `Phase: running`. Two commands answer differently about the same operative. | `info` says `running`, `list` says `degraded`. | **New.** A CLI rendering defect, not a state defect |
| KI-153 | Wiring the same procedure twice is refused by no SDK but Rust and Java — the value is delivered **twice**. Elixir is affected too. | `edge.into(P); edge.into(P)` — two edges onto one procedure. | **Four veneers done** `f7a1aa6b`; the Elixir half is the owner's, deferred by name |
| KI-152 | One uninstalled plugin parks the whole operative and **every plugin after it is silently never started**. No log line, and nothing names which plugin. | Two plugins, the first is missing — the second is never attempted. | **Half done** `7fbff61b` — the operator now sees which plugin. The parking policy is still the owner's decision |

## 2. Places where the SDKs behave differently

*(empty — KI-118 and KI-121 were refuted on 2026-09-02)*

## 3. Things that should be visible and are not

| # | Problem | Example | State |
|---|---|---|---|
| KI-72 | A QUIC connection reports no statistics at all. When something wedges, nobody can see where. | Every counter reads empty, even while traffic flows. | Open, **downgraded** — the silence is closed, only the metrics are owed |

## 4. Tests and tools that mislead

| # | Problem | Example | State |
|---|---|---|---|
| KI-68 | One workflow test spends a full minute waiting after a failure. It slows every full run and proves nothing. | 60 seconds of waiting where nothing more is coming. | **ESCALATED — the owner's decision.** The cause is found and measured; every fix changes production behaviour |
| KI-132 | PHP has two execution modes and no test drives the second one end to end. We do not know whether it works. | The config allows `swoole` mode; no run uses it. | Open |
| KI-145 | A PHP runner may keep running after its namespace is gone. Four other languages exit on their own; PHP does not — but the cause is not yet found. | Three times out of three one PHP process survived 75 seconds; in other languages it was gone in 2. | **Suspicion** — the test is fixed, the runner is undiagnosed |

## 5. Large tracks still ahead

| # | Work | What it means | State |
|---|---|---|---|
| R7 | The last capability — workflows — is missing from five SDKs. Eight changes: the four veneers share one fix, then each gets its fixtures, then three for Elixir. | Without it a workflow cannot be written in those languages at all. | **5/8 DONE. All six FFI SDKs carry workflows.** The Elixir half (6–8) is the owner's, proposal written |
| R9 | Unifying the command-line flags. | — | **DONE** `82fe8fad` — four readers became one; KI-80 and KI-148 both closed |
| R10 | Pre-publication checklist. | Small, framework only. | Waiting |
| R11 | Two specs that mandated something absent from the code. | — | **DONE** `e3ad6b43` — both were stale specs, not code defects |
| R12 | Documentation reference check. | Built last so that it stays green. | Waiting |
| — | About 20 small follow-ups: dead code, help-text lines, one flag that disappears, one metric blind to untracked files. | None is large; only the proposal is unwritten. | Waiting |

## 6. Awaiting the owner's decision

| # | Question | Why it is yours | My recommendation |
|---|---|---|---|
| A | Should a review round check every open entry against today's code before more fixing? | It costs a couple of hours and produces no code. | **Yes.** Of eleven "serious" entries four were already fixed — the list overstates |
| B | Re-record the metric baseline now or at the end of the track? | Writing it needs your permission. | At the end of the track; the old comparison is stricter, so nothing is hidden |
| C | The `.gitignore` rule meant to keep archived proposals does not work. Fix, delete, or leave? | **WITHDRAWN 2026-09-02** — the owner ruled the rule is deliberate: proposals stay out of git, "the spec is what we keep". Do not re-raise |
| D | KI-68: one test spends 60 s on every full run. Change what production waits for because of it? | Every possible fix changes production behaviour, not just the test. | **Not now.** The cause is found and recorded; 60 s for one test does not justify changing drain logic before Friday |

## 7. Owed but blocked

| # | What | Why blocked |
|---|---|---|
| — | *(cleared 2026-09-02)* The 23 missing index lines are written; `CLAUDE.md` and `INDEX.md` were committed at `2aa10068`. | — |

## 8. Resolved during session 10

| # | What | Where |
|---|---|---|
| KI-114 | A test could never pass alongside others and left a process behind. | `983787a9` — the daemon claimed the same state root the test itself held |
| KI-79 | Five CLI commands were said to report success on failure. | **Refuted** — the exit codes are correct; nothing to fix |
| KI-118 | The SDKs were said to allow a shape Rust refuses. | **Refuted** — all four refuse it now |
| KI-121 | An unnamed channel was said to get a different name per language. | **Refuted** — `55b8a748` closed it without naming it |
| KI-129 | A live state query could answer with unwritten state. | **Resolved** — the test race is gone; the semantics stand as ruled |
| KI-141 | The second arm of a fan-out vanished silently on the durable path. | Model `55b8a748`, **proved on the wire in four languages** with a negative control |
| KI-143 | A Python fixture named its channel wrongly. | `47d32c9d` |
| KI-144 | A hybrid channel encoded the next step's message with the wrong encoder; every item to the DLQ. | `6612f8bc` — our own defect, caught at its own checkpoint |
| KI-146 | A runner spawned in `setup_all` was always the Rust one. | `f670106f` |
| KI-147 | All four veneers emitted a workflow setting the core refuses. | `cc1763c3` — **four tests had pinned the defect in place** |
| KI-124 | A Python fixture named two procedures exactly as its twin did. | `b61889f0` |
| KI-117 | A test demanded 1 ms where the contract allows 30 s. | `b61889f0` |
| KI-109 | A shared list compared with exact equality, plus the leak that caused it. | `b61889f0` |
| KI-133 | A PHP shared-memory suspicion — **refuted**, no code written. | `b61889f0` |
| KI-80 | Two ways to ask the same thing, each silently ignoring the other. | `82fe8fad` — 17 of 33 invocations answered wrongly |
| KI-148 | `plugin list --output json` returned a different list, not a different format. | `82fe8fad` — the overload taken apart |
| KI-142 | A dangling transform was silently discarded; both references refuse it. | `732cb6e9` — all four veneers, in the reference's own words |
| KI-115 | Two specs mandated a mechanism that does not exist. | `e3ad6b43` — measured, not guessed |
| KI-149 | Three refusals were missing from four veneers; `DuplicateKeyBy` discarded the author's key silently. | `681669d9` — measured, not guessed |
| KI-150 | A document claimed seven refusals were identical in every SDK. | `681669d9` — the table now has a measured **Where** column |
| KI-151 | A spec demanded a terminator no SDK implements. | `681669d9` + the spec sync at `f7a1aa6b` — 4 scenarios → 7, none dropped |
