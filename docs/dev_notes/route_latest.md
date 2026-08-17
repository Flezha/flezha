# `route_latest` building block (`add-route-latest-building-block`)

- **`c.route_latest(documents, traffic, key_fn)` / `route_latest_named(.., name)`
  (Rust) — `c.routeLatest(Doc.class, documents, traffic, keyFn[, name])`
  (Java)** — an operative-wide document reaches every keyed Entity, one key at a
  time, on that key's OWN next traffic item. Returns `Stream<(String, D)>`
  (Rust) / `Stream<RoutedDocument<D>>` (Java — no tuples, and the runtime needs
  a `Class` token for the routed payload); the consumer keys by that key.
- **Why it exists at all**: a keyed procedure cannot receive a broadcast — every
  keying declaration is total, and in a partitioned member the computed key is
  admitted against the member's key-group range.
- **THE load-bearing rule — deliver on the key's own traffic, never a
  remembered-set fan-out.** A remembered key set outlives key-group ownership
  (a rescale, or ONE out-of-range item seen during a reconcile window), so
  fanning out over it parks later deliveries against the partition guard.
  Deliver-on-own-traffic makes over-delivery impossible by construction. Price:
  one tick of latency per key, and a key whose traffic stopped is never
  delivered to. That is also why there is no retained broadcast to late-born
  Entities and no rescale-time replay to specify.
- **Sugar, not a runtime feature**: a `merge` into ONE packaged single-entity
  procedure with `{document, version, delivered}` journaled state. No wire
  change, no new replay path. The Rust packaged types are `pub(crate)` — the two
  methods are the whole surface, which is what keeps the guarantee
  un-bypassable; Java has no crate privacy and the runtime instantiates a
  Procedure reflectively from a public class, so `LatestDocumentRouter` is
  public and says so in its javadoc.
- **GOTCHA (Java, the real blocker) — the state codec was built from a freshly
  instantiated Procedure, so the wiring could not register anything.**
  `ActivationDispatcher.JavaInstance` now builds `KryoCodec` from
  `desc.kryoTypes()` (the ProcedureDescriptor's list, which was DEAD — copied
  from `probe.kryoTypes()` at registration and never read) instead of
  `instance.kryoTypes()`. Identical for every existing procedure (both are a
  no-arg instance's constant list); it is what lets `routeLatest` append the
  document's own class, which only the wiring knows.
- **GOTCHA — `ProcedureTestHarness.replayEquals()` builds its codec from
  `procedure.kryoTypes()` alone** (it has no topology), so a Java harness test
  of the router must use a document type Kryo registers by default —
  `RouteLatestRouterTest` uses opaque `String` documents. A typed document is
  covered by `RouteLatestExpansionTest`, which asserts the registration.
- **LIMIT (Java only, documented, not fixed)**: a HOT-SWAP migration moves state
  as JSON, and the retained document is an `Object` field, so a typed document
  comes back as a generic map. Crash-resume, compaction and rebind all restore
  through Kryo and are unaffected.
- **GOTCHA — two routers over one traffic stream is refused, not silently
  merged.** Unnamed mentions of one class are ONE node, so the second
  declaration re-wires the traffic edge and trips `EdgeAlreadyWired` /
  `edgeAlreadyWired` naming `route_latest`. Name them. One channel used as BOTH
  streams trips `DuplicateMergeSource`.
- **Reuse**: `Stream::map` is now the owned-clone spelling of the new
  `map_ref(what, f)` (one closure-erasure site); `Merge::wire` /
  `MergeImpl.wire` became the shared named/unnamed merge-closing seam both
  `into` spellings and the block use. The key function is an EDGE transform, so
  the traffic payload is replaced by its key and never reaches the router.
- **Tests**: `sdk/rust/src/channel/route_latest/tests.rs` (unit, beside the
  private types: 5 spec scenarios + `replay_equals`/`replay_emits_equal` +
  expansion + both refusals), Java `RouteLatestRouterTest` (delivery contract)
  and `RouteLatestExpansionTest` (topology, kryo registration, refusals).
- **Docs**: `docs/sdk_authoring_contract.md` §4.1, both READMEs, root +
  `sdk/rust/CHANGELOG.md`, `sdk/rust/public-api.txt`.

