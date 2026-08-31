# `sdk/java` refactor (`refactor-4-java-sdk`)

Area 4 of the refactor programme, six worker segments (A–F) with a supervisor
commit at each checkpoint. Zero behaviour change. What follows is what a later
session would otherwise rediscover; the plan itself is in the archived change.

## What moved, and by what rule

**Four files over the 800-line ceiling became zero, and each split by owner.**

- `StreamOperativeTopologyBuilder` **1 484 → 761** plus nine files (segment B).
  `TopologyRules` holds the eight rule families in the order they run;
  `TopologyMaterializer` builds the topology; the DSL handles (`EdgeStreamImpl`,
  `EntryChannelImpl`, `NodeEdge`, `StreamImpl`, `MergeImpl`, `EdgeSite`) and
  `PayloadCodecs` are their own files.
- `FlezhaProcessor` **1 075 → 148** (segment C), into `ProcedureGenerator`,
  `StateGenerator`, `ProcedureModel`, `StateModel` and `TypeMirrors`.
- `ActivationDispatcher` **993 → 68** (segment D), split by *the state it
  mutates*: `OperativeInstance` (the per-operative cells), `EmitContext` (the
  emit funnel), `EntityRouting`, `StateExchange`, `WorkflowDispatch`,
  `EdgeTransforms`, `EntityCell`, `SessionWorkflowCtx`.
- `ActivationDispatcherTest` **1 392 → five files** over one shared
  `DispatchTestBase` (segment E), 29 tests preserved as 3 + 8 + 8 + 5 + 5.

At the close: `files_over_800` **4 → 0**, `functions_over_50` **10 → 0**,
`long_names` **1 → 0**, `narrative_markers` **6 → 0**, `sleeps` **6 → 1** (the
survivor is `PollHostTest.awaitTrue`'s 10 ms poll interval, kept deliberately —
design D9 — where the other five went by being replaced with observations,
not shorter waits), credo 0, clippy 0.

## Java's package rule decided the layout, not taste

`design.md` asked for a `builder/dsl/` sub-package. **Java's package-private
access does not cross into a sub-package**, so the directory and the
encapsulation are mutually exclusive: `builder.dsl` costs seven members and two
types going `public`, against an area whose whole proof obligation is an *empty*
public-signature diff. Ruled at segment B: encapsulation wins, the design was
corrected. Nested-private → top-level package-private is the same bytecode on
Java 11 nestmates, so the move itself is free.

## The ruler was corrected before anything was measured against it

Batch 0 (segment A) fixed `refactor_metrics.py`'s Java classifier, which counted
a **type header and a constructor** as functions. The guard was proved by
diffing the complete Java function list across all 251 files, old classifier
against new: **84 removals, 0 additions, 0 unexplained**. Repo-wide effect: one
area moved (`sdk/java` `functions_over_50` 11 → 10, `long_names` 7 → 1). A
metric diff shows what moved; a full-list diff shows that nothing moved for a
reason nobody checked.

## jscpd counts *pairs*, and a split moves the number in both directions

`.jscpd.json` runs `mode: weak` at `minTokens: 40`, which matches four to six
consecutive long-FQN **import** runs against *different* identifiers. Most of
segment B's new pairs are 41–52-token import runs between files in one package —
not copied code, and `removeUnusedImports` is configured and ran. `design.md`'s
stated cause (a copied import block) was refuted by measurement; the real one is
the token window.

The reverse bites too. Collapsing twelve in-file copies of a fixture into **one**
shared base *raises* the count, because jscpd only sees cross-file pairs:
nineteen of segment E's twenty-four new pairs are the same clone re-anchored at
its new single home. `duplicate_pairs` closed at **92** against 81, and the real
reduction is invisible to the metric. Always name the scan set — the recorded
number is the **full-scan** one, and `--check` refuses a baseline taken over a
different set rather than differencing two numbers that mean different things.

## The measurement, and what it cannot say

`J1` = `mix bench --shape regression --runtime java -n 100 -m 10000`, end-to-end
window, arm A (`refactor-baseline-4`) against arm F, jars swapped one file at a
time, natives sha-identical on both arms.

Its floor was **declared before measuring** at "over 6 % and it blocks nothing",
then measured at **11.61 %** end-to-end and **14.47 %** receive over five arm-A
runs — so J1 was reclassified `guard` → `direction` in the same edit. The five
readings rose monotonically, which is a thermal trend on this laptop rather than
scatter, and drift cancels in an alternating design where an absolute floor
cannot. Hence: interleaved, paired, on mains power.

Five paired rounds, **three A-first and two F-first** — the reversal exists
because all three of the first rounds ran A second and would have confounded a
second-position thermal penalty with the arm:

| round | order | A e2e ms | F e2e ms | F/A |
|---|---|---|---|---|
| r1 | A,F | 4681.9 | 4592.5 | −1.91 % |
| r2 | A,F | 4666.8 | 4820.6 | +3.30 % |
| r3 | A,F | 4929.6 | 5436.1 | +10.27 % |
| r4 | F,A | 4697.2 | 4599.2 | −2.09 % |
| r5 | F,A | 4788.1 | 4828.0 | +0.83 % |

**No direction detected.** The sign flips in both orderings; arm F holds both the
fastest single reading of the ten and the slowest. Geometric mean of the paired
ratios **+1.98 %**, median of arms +2.63 %, and **+0.03 %** with each arm's
slowest reading dropped. The paired deltas span 12.36 points — wider than the
row's own floor. J1r (receive, reported and never a verdict): +2.70 %, +1.76 %
trimmed, against a 14.47 % floor.

**A clean row here is not "no cost", and the note says so on purpose.** J1 can
catch the categorical failures this split risks — a megamorphic call site, a
lost cache, an allocation moved onto the per-record path — because those are
2-5× effects that 11.6 % of noise cannot hide. It **cannot** certify the absence
of a small regression: `grep -rn jmh` over every pom, workflow and script in this
repository is **empty**, no row here resolves the ≤ 2 % a JVM class extraction
plausibly costs, and the point estimate above sits inside exactly that
unresolvable band. KI-34 owes that instrument.

**The evidence that the hot path did not regress is structural, not measured**,
and it is segment D's: `EntityRouting` and `WorkflowDispatch` are constructed
once per materialized operative (**2 objects per operative, 0 per record**),
`StateExchange` and `EdgeTransforms` are static-only, `EmitContext` kept its
single reference and its eleven fields; no interface was introduced, so every
new type is `final` with one implementation and every call stays monomorphic;
and every cached lookup is enumerated and preserved — the `identityBytes` cache,
the shared `edgeCodec`, the `procedureId`-indexed lists, both `ConcurrentHashMap`s
and the static `ObjectMapper`.

**J2 was not run.** The entity shape stalls at `--pool-sizes` 1/2/4 on this box
and the row has never been floored; a row that cannot gate spends the machine
for a number nobody may act on.

## Zero behaviour change, proved four ways

1. **Public-signature diff.** `javap -protected` over every `com/flezha/sdk`
   class extracted from the two arm jars: **163 public types on each side**,
   exactly one renamed (`ActivationDispatcher$JavaInstance` →
   `OperativeInstance`, members identical) and exactly one member changed
   (`ActivationDispatcher.instances()`, whose return type names it). Every other
   entry in the raw diff is a nested package-private class becoming a top-level
   package-private class.
2. **String literals ≥ 12 characters**, comments excluded, tag vs close: 592 →
   526. Per module, `flezha-sdk-api`, `-runtime`, `-native`, `-testing` and
   `-examples` are **0 lost and 0 gained** — every runtime and refusal message
   in the SDK is byte-identical. All **95** lost literals lived in
   `FlezhaProcessor.java`; all 29 gained live in the files it became
   (`StateGenerator` 27, `ProcedureGenerator` 1, `TypeMirrors` 1,
   `FlezhaProcessor` 1). They are code-generation *templates* re-cut by the
   twelve-emitters-into-three fold, and the emitted text is proved identical
   rather than argued: driving the processor over a branch-complete 26-source
   corpus before and after gives **29 files, 738 lines, `diff -r` identical, and
   all 35 javac diagnostics identical including `file:line`**, on both the good
   and the refusal corpus.
3. **`test/fixtures/host_class_decl_keys.txt` untouched** (`git diff` empty) and
   `HostClassDeclarationTest` green.
4. **The suites.** Java `mvn -P coverage,native-verify clean verify`: **209
   tests, 0 failures** (208 at the tag; the extra one is coverage — segment E
   parameterised four methods that drove five spellings, so the diagnostics
   check now runs on all five). `mix test`: **1 872 passed, exit 0, zero
   uncaptured-log gate hits**. `FLEZHA_E2E_RUNTIME=java mix test --include
   system`: **1 952 / 1 953**, the one failure KI-57's `sun_path` overflow, whose
   message is in the log.

## The line count went up 310, and the ceiling is why

`sdk/java` production **8 457 → 8 829 (+372)**, test **5 276 → 5 214 (−62)**,
**net +310** against `design.md`'s ~150 estimate. **The estimate was wrong, not
the work**: every segment accounted its lines by cause, and every cause is
charter §7 ceiling cost — file headers and imports, inner classes gaining an
`owner` field, the explicit parameter lists an extracted module needs where a
nested class read the enclosing instance's fields, and the accessors that go
with them. Per segment: B **+198**, C **+78**, D **+84**, E **+12** production
with **−62** test.

Charter §1 anticipates exactly this: *"Clearing a ceiling and reducing total
lines pull against each other; when they do, the ceiling wins and the area's
total is what must fall."* The ceiling won five times and every ceiling in
`sdk/java` is now zero.

**Funding was found where it existed and refused where it would have cost
something real.** Segment C took ≈ **−127** against its +205 — twelve
`StateGenerator` emitters folded into three, `TypeMirrors` folding four copies of
*"erasure equals X, arity N"* onto one probe, `classLit` deleted outright as
dead. Segment E took **−50** — fifteen fixtures that were fifteen copies of
`factoryId()` + `channels()` became one base, and the seven procedure fixtures
that were the twelve jscpd clones became another. Segment D **refused three
candidates and recorded why**: merging the two run prologues needs a record *per
record*, a lambda over `prepareKeys`' arms captures `edge`, and hoisting the
`LinkedHashSet` allocates when neither branch is taken — each would have put an
allocation on the per-record path to save a handful of lines. Segments D and E
both left ~50 lines of **dead public surface** standing (KI-82) for the same
reason: every one of them is `public`, and this area's contract is an empty
`javap` diff. Refusing funding for a stated reason is worth more than the lines.

What the 310 bought: every ceiling at zero, and a hot path that is now **eight
separately constructible pieces with explicit dependencies** where it was
private nested classes inside a 993-line file reachable only through
`dispatch(Activation)` — which is the precondition for the JMH module KI-34
needs.

## What the flip means

`sdk/java` is under `blocking` in `docs/refactor/baseline.json` from this commit,
re-recorded from the landed tree over the full scan set: `prod_loc` **8 829**,
`test_loc` 5 214, `files_over_800` **0**, `functions_over_50` **0**,
`long_names` **0**, `narrative_markers` **0**, `sleeps` **1**,
`duplicate_pairs` **92**, `cycles` 0, credo 0, clippy 0. Any later rise in one of
those fails `--check` naming the area, the metric and the file.

Charter §9's *"time is a ratchet"* now reaches this area too: `mvn_test_sdk_java`
is recorded at **30.0 s**, the wall clock of
`mvn -B -f sdk/java/pom.xml -P coverage,native-verify clean verify` on this box
(two runs, 29.10 s and 29.12 s of Maven's own total). It had no timing key before.

## Traps the next session should not re-pay

- **`--write-baseline` without `--lint` writes every `credo_warnings` and
  `clippy_warnings` as `null`**, and `--check` skips a null on either side — so
  every credo ratchet silently stops blocking. Verify after writing: 36 areas,
  0 credo nulls, repo credo 378.
- **`--timing` replaces the whole timings map** rather than merging into it
  (`data[key] = given or baseline.get(key, {})`), so recording one new key on the
  command line drops the keys that were already there. `mvn_test_sdk_java` was
  therefore written into the recorded file directly, leaving `mix_test_exunit`
  and `mix_test_wall` on the stamps they were measured under.
- **Never `-P native-build`** for a test-path rebuild: it needs the network and
  it can rewrite the tracked `THIRD_PARTY_NOTICES.md`. The conformance
  manifest's own `native-verify` is the right profile, and
  `sha256sum`-ing the notices before and after is how you know.
- **KI-18 fires after a maven build.** It did again here — `flezha_test.exs:389`
  in the Java smoke tier, one failure in 1 895 — and **arm A, built at the tag,
  failed the same module at `:419`**, which is what makes it the box rather than
  the change. Confirm the test count did not move, record the sighting, move on.
- **Two gates are red at HEAD and neither is this area's**: `format-check.sh`
  exits 1 because `php-cs-fixer` crashes on a vendor tree above the PHP floor
  `composer.json` declares and the gate reports that as *unformatted* (KI-76),
  and `check-kind-naming.sh` exits 1 on 11 prose hits, every one in `docs/`
  (KI-65). `mvn spotless`, prettier, ruff, `cargo fmt`, `mix format`, reuse and
  the notices check are all clean.
- **Left standing on purpose**: KI-55 (its test crossed the split
  byte-identical, still asserting the wrong value deliberately, and its javadoc
  was carried verbatim rather than corrected — that sentence is the entry's to
  fix), KI-74, KI-75, KI-82, KI-34.
