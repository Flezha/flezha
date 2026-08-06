# flezha-sdk-testing

Test fixtures and harnesses for Flezha Java SDK Procedure authors.

## ProcedureTestHarness

Drive a `Procedure` through a sequence of inputs without standing up the runtime.
Records the `state.apply`-ed Changes and the `ctx.emit`-ed values; asserts
state determinism via Kryo byte-equality replay.

```java
import com.flezha.sdk.testing.ProcedureTestHarness;

ProcedureTestHarness.of(new CounterProcedure())
    .applyInputs(List.of(1L, 2L, 3L))
    .expectChanges(List.of(new CounterChange(1), new CounterChange(2), new CounterChange(3)))
    .expectState(new CounterState(6));

assertThat(ProcedureTestHarness.of(new CounterProcedure())
    .applyInputs(List.of(1L, 2L, 3L))
    .replayEquals()).isEmpty();  // determinism gate
```

`replayEquals()` returns `Optional.empty()` when the Procedure's state is a
deterministic function of the Change log (the umbrella's correctness
contract); otherwise it returns `ReplayMismatch` carrying the diagnostic bytes.
