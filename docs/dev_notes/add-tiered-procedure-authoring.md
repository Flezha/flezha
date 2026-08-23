# Tiered procedure authoring (`add-tiered-procedure-authoring`)

- **Three tiers, explicit always wins.** Sugar now covers multi-emit; Java's
  five type tokens default from the `implements` clause; Rust gains a
  `SimpleProcedure` shorthand. The mandatory hand-written surface is now
  `name` + `init_state` + `run` in BOTH SDKs.
- **`emits = T` (Rust) / `emits = T.class` (Java) is NOT optional decoration —
  it is the only place the output type can come from on the ctx shape.**
  Channel payload types come from the producer's declared `Output`
  (`builder.rs`: `output_type_id = TypeId::of::<S::Output>()`), and a
  context-parameter `run` returns unit, so without `emits` the channel would be
  typed `()` while the body emits `Candle`. Refusals: value-return + ctx,
  `key` + ctx, `emits` without ctx — all named, in both SDKs.
- **GOTCHA (Rust, the real blocker) — `SimpleProcedure::run` carries
  `where Self: Procedure`.** `StateHandle<'_, Self>` projects
  `<Self as Procedure>::State`/`::Change`, so the trait needs `Self: Procedure`
  — but a SUPERTRAIT would cycle with `impl<T: SimpleProcedure> Procedure for T`.
  A method-level where-clause is checked at the call/impl site instead, which
  breaks the cycle. `Self::Input` must also be spelled
  `<Self as SimpleProcedure>::Input` (E0221: both traits define `Input`).
- **`impl DerivedState for ()` is new** (`Change = NoChange`) — it is what makes
  `type State = ()` the stateless spelling in the tier, instead of needing a
  second trait for stateless procedures.
- **LIMIT — the Rust tier cannot override `on_timer` or `compensate`.**
  Registering a timer from a `SimpleProcedure` panics at runtime (that hook's
  default panics and the tier has no seam to override it). Documented in the
  `simple.rs` module docs; the fix is to implement `Procedure` directly.
  Coherence enforces "not both" for free (E0119), pinned by
  `tests/ui/simple_procedure_is_not_also_a_procedure.rs`.
- **Java: `TypeTokens` (package-private, `com.flezha.sdk.procedure`)** walks
  `getGenericInterfaces()`/`getGenericSuperclass()` substituting type variables
  at every hop, cached per class in a `ClassValue`. Unresolvable ⇒
  `StreamOperativeConfigurationException.unresolvedTypeToken` naming class +
  parameter. It fires at `defineStream`/`defineWorkflow` because
  `StreamOperativeTopologyBuilder.registerNode`'s `probe` already reads all five
  tokens — no new call site was needed.
- **GOTCHA — `kryoTypes()` defaults to `[change, state]` ONLY when the change is
  not `NoChange`.** Kryo ids are positional and persisted; defaulting
  unconditionally would register a phantom `NoChange` id for every
  persistence-free procedure and change what an existing declaration means. The
  carve-out makes the new default reachable only where the empty list was
  already wrong (a real Change would have thrown at serialize time). Explicit
  `kryoTypes()` declarations were deliberately LEFT alone everywhere — several
  are `[state, change]` or include `Input`, and their order is the id table.
- **`FlezhaProcessor` no longer generates the five token methods.** The
  generated class's `implements Procedure<…>` clause states them, and the
  interface defaults read it — the same path a hand-written procedure takes, so
  §1 escape-hatch equivalence stays true.
- **222 token overrides removed across THREE corpora** — `sdk/java` (157),
  `test/apps/java_runner` (65, which is why the dist jars were rebuilt), plus
  `LatestDocumentRouter`. The deletion criterion is sound, not heuristic: a
  method declared `public Class<X> fooType() { return X.class; }` proves `X` is
  a concrete class, because a type variable cannot be `.class`'d. KEPT
  deliberately: `StreamOperativeTopologyBuilder.UNINSTANTIABLE` (its contract is
  *never throw*, so the build keeps collecting errors) and
  `ProcedureDefaultsTest.OverridingStillGeneric` (the "explicit wins" fixture).
- **GOTCHA — `mvn test -pl flezha-sdk-processor` without `-am` resolves
  `flezha-sdk-api` from the stale installed jar**, surfacing as
  `NoSuchMethodError: Procedure.emits()` inside the annotation processor. Same
  trap as every other api-module change; always `-am`.
- **FIXED IN PASSING — the `Result`-returning sugar arms had an uninferrable
  error type.** `(#user_body)?` left `E` unconstrained (`E0282`/`E0283` the
  moment a body ended in a bare `Ok(())`). All three Result arms now bind the
  body under the author's DECLARED return type first, so `?` inside it resolves
  against that error type. The six-variant `Output` enum collapsed to
  `Output { shape: Shape, result: bool }` in the process — the `?`-unwrap and
  the annotation exist once instead of three times.
- **Tests**: Rust `sdk/rust/tests/sugar_emit.rs` (renamed from
  `sugar_keyed_emit.rs`; the sugared 3-emit body vs its hand-written twin),
  `sdk/rust/tests/simple_procedure.rs` (own `[[test]]`,
  `bench-internals,testing`), four new `tests/ui/` fixtures; Java
  `ProcedureDefaultsTest` (direct / through-a-generic-base / unresolvable /
  override-wins / kryo default), `FlezhaProcessorTest` (ctx generation + three
  refusals), `StreamOperativeTopologyBuilderTest.unresolvedTypeTokenRejected`.
  E2E: `test/java_sdk/matrix_test.exs` (28) + `compaction_e2e_test.exs` run
  against the rebuilt, ceremony-stripped runner app.
- **Docs**: `docs/sdk/authoring_contract.md` §2.1 (the context shape) and §2.2
  (the three tiers), `docs/sdk/state_persistence.md`, both READMEs,
  `sdk/rust/public-api.txt`, root + `sdk/rust/CHANGELOG.md`.
  `docs/sdk/rust_operative_architecture.md`'s Java sample was left alone — it is already
  stale in a larger, unrelated way (3 type parameters instead of 6).

