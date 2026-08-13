# Java SDK — Per-Entity State Persistence

The Java authoring surface for durable Entity state: six-parameter `Procedure`
generic, `StateHandle<State, Change>` mutation chokepoint,
`StreamOperative::persistenceEnabled` default-method, Kryo serialization with
stable registered type IDs.

This doc is the Java-author counterpart to
`docs/operative_state_persistence.md` (Rust). The wire format is byte-identical
across SDKs; the header `sdk_lang` byte differs (Rust = 1, Java = 2) and
the `format_id` byte differs (postcard = 1, Kryo = 4). Cross-language load
fails loud.

**Where the machinery lives.** Everything below the authoring surface — the
flush cadence, the effect record, the credit window, restore, compaction — is
the shared runner core's, not Java's. Java supplies the Kryo-encoded change
bytes and the core does the rest, which is why this page describes types you
write and not threads you own. See `docs/sdk_poll_core.md`.

## Surface

### Procedure generic shape

```java
public final class CounterProcedure
        implements Procedure<Long, CounterProcedure.State, Void, CounterChange, WallClock, Void> {
    public static final class State { public long value; }

    @Override public String name() { return "counter_procedure"; }
    @Override public Class<Long> inputType() { return Long.class; }
    @Override public Class<Void> outputType() { return Void.class; }
    @Override public Class<CounterChange> changeType() { return CounterChange.class; }
    @Override public Class<WallClock> clockType() { return WallClock.class; }
    @Override public Class<Void> configType() { return Void.class; }
    @Override public List<Class<?>> kryoTypes() { return List.of(State.class, CounterChange.class); }

    @Override public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> payload) {
        return new State();
    }

    @Override public void applyChange(State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override public void run(Long input, StateHandle<State, CounterChange> state, ProcedureCtx ctx) {
        state.apply(new CounterChange(input));
    }
}
```

Six type parameters are required because Java type erasure does not preserve
associated-type bounds. The `Class<…>` reflection methods (`changeType()`,
`clockType()`, `configType()`) recover the erased types at runtime for Kryo
registration and flusher dispatch.

### StateHandle

`StateHandle<State, Change>` replaces the raw `State` parameter on
`Procedure.run`. Reads go through `state.get()`; the recorded, replayable
mutation path is `state.apply(change)`.

**Enforcement is by convention, not structure.** `state.get()` returns the live
`State` object, so `state.get().field = x` compiles and runs. That write is
never journaled as a Change, so it is **captured by graceful shutdown and live
migration** (both snapshot the live object) and **lost by any restore**, which
rebuilds state from a snapshot plus the change log. It therefore survives dev,
staging, and every test, and disappears exactly when persistence is supposed to
save it — under an unplanned crash.

Rust forecloses this at compile time: its `StateHandle` implements `Deref` and
deliberately not `DerefMut`, so a direct field mutation does not compile (see
`docs/operative_state_persistence.md`). Java cannot match that — `get()` must
return a readable object, and there is no borrow checker to withhold `&mut`.
This is the same class of Rust asymmetry the Java SDK already documents for
erasure vs `EventTimeFrom` (`Procedure.eventTime`): the guarantee is enforced
structurally in Rust and by a weaker mechanism in Java.

The detection Java provides in place of enforcement is
`ProcedureTestHarness.replayEquals()` — an **opt-in** determinism gate that
replays the recorded change log against a fresh baseline and reports a mismatch
when the live state diverges from the change-only replay. A bypassed mutation
is exactly what it catches. Run it in CI against your own Procedures.

#### State-mutation enforcement, per SDK

The authoring contract records the enforcement level each SDK actually achieves,
rather than each claiming the strongest one (see
`openspec/specs/sdk-authoring-contract`):

| SDK    | Enforcement level | Mechanism |
| ------ | ----------------- | --------- |
| Rust   | Structural (compile-time) | `StateHandle: Deref` without `DerefMut`; `init_state` returns state by value |
| Python | Runtime-rejected | direct attribute assignment on durable state raises, naming the attribute |
| Java   | Documented-only + opt-in detection | convention; `ProcedureTestHarness.replayEquals()` catches a bypass |

Java does **not** add a runtime guard: withholding the object (a defensive copy
or proxy on every `get()`) would be a hot-path cost imposed on every read to
defend against a compile-time-shaped problem, and `get()` must still hand back
something readable. The asymmetry is deliberate and documented, not an omission.

### Change and CoalesceKey

```java
public record CounterChange(long delta) implements Change {}

public record TickChange(int interval, long ts, long delta) implements Change {
    @Override public Optional<CoalesceKey> coalesceKey() {
        return Optional.of(CoalesceKey.fromHash(interval, ts));
    }
}
```

Changes with equal coalesce keys collapse to the latest within a flush
window. Changes without a key are emitted as-is.

### NoChange

`NoChange` is uninhabited. Procedures opting out of persistence declare
`Change = NoChange.class`; the runtime detects
`Procedure.changeType() == NoChange.class` and produces no change bytes, so
nothing is registered for flushing.

### Clock

`Clock` is a sealed interface permitting `WallClock`, `LogicalClock`, and
`EventTimeFrom<Input>`. All three are wired. Because erasure cannot express
Rust's `TimeExtractor<Input>` bound, an `EventTimeFrom` Procedure that does not
override `eventTime` — and a `LogicalClock` Procedure declaring
`Change = NoChange` — are refused at **registration** rather than compile time.

### Flushing

There is no Java-side flusher. Changes ride the activation result back into the
runner core, which coalesces, builds the effect record, and drives the linger
cadence (`FLEZHA_FLUSH_LINGER_MS`, default 5 ms) for every host language. When
`persistenceEnabled = false` nothing is serialized or sent.

### Kryo registration

```java
@Override public List<Class<?>> kryoTypes() {
    return List.of(CounterProcedure.State.class, CounterChange.class);
}
```

Stable type IDs are essential for cross-version compatibility under additive
Change evolution. Append-only: adding a new Change variant appends a new
class to the END of the list, preserving prior IDs across SDK versions.
Reordering is fragile; documented contract.

### Wire format

The header is 16 bytes, big-endian:

```
0:1   sdk_lang        (Java = 2)
1:5   sdk_version     (u32; from Manifest Implementation-Version)
5:9   schema_version  (u32; from Procedure.schemaVersion())
9:10  format_id       (Kryo = 4)
10:12 reserved
12:16 crc32           (over change_bytes)
```

A header with `sdk_lang != 2` raises `StateLoadException.sdkLangMismatch`;
`format_id != 4` raises `StateLoadException.formatNotSupported`. Both fail
loud per umbrella Decision 9.

## Test harness

`flezha-sdk-testing` exports `ProcedureTestHarness<I, S, O, C>` for unit-testing
Procedures without standing up the runtime:

```java
ProcedureTestHarness.of(new CounterProcedure())
    .applyInputs(List.of(1L, 2L, 3L))
    .expectChanges(List.of(new CounterChange(1), new CounterChange(2), new CounterChange(3)))
    .expectState(new CounterState(6));

assertThat(ProcedureTestHarness.of(new CounterProcedure())
    .applyInputs(inputs)
    .replayEquals()).isEmpty();  // determinism gate
```

`replayEquals()` returns `Optional.empty()` when the Procedure's state is a
deterministic function of the Change log; otherwise returns
`ReplayMismatch` carrying the diagnostic Kryo bytes.

## Trade-offs

- The six-parameter `Procedure` generic is verbose. Documented; users learn
  the pattern once. The alternative (no generics, runtime type checks) is
  worse. See `operative-state-persistence/design.md` Decision 1.
- `Kryo.setRegistrationRequired(true)` is intentional: unregistered types
  fail loud at first serialization. Users add to `Procedure.kryoTypes()`.
- Changes reach the core only when the activation completes, so a long
  invocation — including a GC stop-the-world pause inside one — delays the
  record behind it. The durability bound is a guideline, not a hard cap; tune
  G1 / ZGC / Shenandoah when it has to hold tightly.
- `Optional<RestoreInitPayload>` adds a per-restart allocation vs Rust's
  `Option<&RestoreInitPayload>`. Negligible (microseconds per Entity
  restart).

## Related docs

- `docs/operative_state_persistence.md` — Rust-side per-Entity persistence
- `docs/resilience/state_persistence.md` — controller-side WAL and time-travel
- `docs/sdk_protocol_spec.md` — wire-format spec
- `docs/operative_architecture.md` — overall Operative/Procedure/Entity model
