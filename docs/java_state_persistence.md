# Java SDK — Per-Segment State Persistence

Java SDK port of `operative-state-persistence-rust`. Mirrors the Rust trait
surface one-for-one within Java idioms: six-parameter `Operation` generic,
`StateHandle<State, Change>` mutation chokepoint, `UnboundedOperative::persistenceEnabled`
default-method, per-Segment flusher, Kryo serialization with stable
registered type IDs.

This doc is the Java-author counterpart to
`docs/operative_state_persistence.md` (Rust). The wire format is byte-identical
across SDKs; the header `sdk_lang` byte differs (Rust = 1, Java = 2) and
the `format_id` byte differs (postcard = 1, Kryo = 4). Cross-language load
fails loud per umbrella Decision 9.

## Surface

### Operation generic shape

```java
public final class CounterOperation
        implements Operation<Long, CounterOperation.State, Void, CounterChange, WallClock, Void> {
    public static final class State { public long value; }

    @Override public String name() { return "counter_operation"; }
    @Override public Class<Long> inputType() { return Long.class; }
    @Override public Class<Void> outputType() { return Void.class; }
    @Override public Class<CounterChange> changeType() { return CounterChange.class; }
    @Override public Class<WallClock> clockType() { return WallClock.class; }
    @Override public Class<Void> configType() { return Void.class; }
    @Override public List<Class<?>> kryoTypes() { return List.of(State.class, CounterChange.class); }

    @Override public State initState(Optional<String> operationKey, Optional<RestoreInitPayload> payload) {
        return new State();
    }

    @Override public void applyChange(State state, CounterChange change) {
        state.value += change.delta();
    }

    @Override public void run(Long input, StateHandle<State, CounterChange> state, OperationCtx ctx) {
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
`Operation.run`. Reads go through `state.get()`; the recorded, replayable
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
erasure vs `EventTimeFrom` (`Operation.eventTime`): the guarantee is enforced
structurally in Rust and by a weaker mechanism in Java.

The detection Java provides in place of enforcement is
`OperationTestHarness.replayEquals()` — an **opt-in** determinism gate that
replays the recorded change log against a fresh baseline and reports a mismatch
when the live state diverges from the change-only replay. A bypassed mutation
is exactly what it catches. Run it in CI against your own Operations.

#### State-mutation enforcement, per SDK

The authoring contract records the enforcement level each SDK actually achieves,
rather than each claiming the strongest one (see
`openspec/specs/sdk-authoring-contract`):

| SDK    | Enforcement level | Mechanism |
| ------ | ----------------- | --------- |
| Rust   | Structural (compile-time) | `StateHandle: Deref` without `DerefMut`; `init_state` returns state by value |
| Python | Runtime-rejected | direct attribute assignment on durable state raises, naming the attribute |
| Java   | Documented-only + opt-in detection | convention; `OperationTestHarness.replayEquals()` catches a bypass |

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

`NoChange` is uninhabited. Operations opting out of persistence declare
`Change = NoChange.class` (the default in the legacy migration); the runtime
detects `Operation.changeType() == NoChange.class` and skips per-Segment
flusher registration entirely.

### Clock

`Clock` is a sealed interface permitting `WallClock`, `LogicalClock`, and
`EventTimeFrom<Input>`. v1 fully wires `WallClock` only; declaring the
others fails operative registration with
`OperativeConfigurationException.clockNotImplementedYet`.

### Per-Segment flusher

Single platform thread per JVM, lazy-started on first non-`NoChange` Operation
registration. Wakes every `linger_ms` (system property
`flezha.flush.linger.ms` &gt; env var `FLEZHA_FLUSH_LINGER_MS` &gt; default
5) and drains every registered Segment. Toggle-aware: when
`persistenceEnabled = false`, drain-and-discard without serialization or
wire I/O.

### Kryo registration

```java
@Override public List<Class<?>> kryoTypes() {
    return List.of(CounterOperation.State.class, CounterChange.class);
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
5:9   schema_version  (u32; from Operation.schemaVersion())
9:10  format_id       (Kryo = 4)
10:12 reserved
12:16 crc32           (over change_bytes)
```

A header with `sdk_lang != 2` raises `StateLoadException.sdkLangMismatch`;
`format_id != 4` raises `StateLoadException.formatNotSupported`. Both fail
loud per umbrella Decision 9.

## Test harness

`flezha-sdk-testing` exports `OperationTestHarness<I, S, O, C>` for unit-testing
Operations without standing up the runtime:

```java
OperationTestHarness.of(new CounterOperation())
    .applyInputs(List.of(1L, 2L, 3L))
    .expectChanges(List.of(new CounterChange(1), new CounterChange(2), new CounterChange(3)))
    .expectState(new CounterState(6));

assertThat(OperationTestHarness.of(new CounterOperation())
    .applyInputs(inputs)
    .replayEquals()).isEmpty();  // determinism gate
```

`replayEquals()` returns `Optional.empty()` when the Operation's state is a
deterministic function of the Change log; otherwise returns
`ReplayMismatch` carrying the diagnostic Kryo bytes.

## Trade-offs

- The six-parameter `Operation` generic is verbose. Documented; users learn
  the pattern once. The alternative (no generics, runtime type checks) is
  worse. See `operative-state-persistence-java/design.md` Decision 1.
- `Kryo.setRegistrationRequired(true)` is intentional: unregistered types
  fail loud at first serialization. Users add to `Operation.kryoTypes()`.
- The flusher's single-thread cadence is bounded by JVM GC stop-the-world
  pauses. Tune G1 / ZGC / Shenandoah for strict durability; the umbrella's
  durability bound is a guideline, not a hard cap.
- `Optional<RestoreInitPayload>` adds a per-restart allocation vs Rust's
  `Option<&RestoreInitPayload>`. Negligible (microseconds per Segment
  restart).

## Related docs

- `docs/operative_state_persistence.md` — Rust-side per-Segment persistence
- `docs/resilience/state_persistence.md` — controller-side WAL and time-travel
- `docs/sdk_protocol_spec.md` — wire-format spec
- `docs/operative_architecture.md` — overall Operative/Operation/Segment model
