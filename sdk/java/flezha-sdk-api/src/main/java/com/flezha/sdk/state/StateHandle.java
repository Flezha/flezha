package com.flezha.sdk.state;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * The handle through which a procedure reads and mutates its entity's state.
 *
 * <p>Read with {@link #get()}. Mutate only with {@link #apply(Change)}, which records the change so
 * it can be replayed:
 *
 * <pre>{@code
 * @Override
 * public void run(String input, StateHandle<Counter, Increment> state, ProcedureCtx ctx) {
 *     state.apply(new Increment(ctx.now()));      // recorded, survives a restart
 *     ctx.emit("count=" + state.get().count);     // reads the value just applied
 * }
 * }</pre>
 *
 * <p>{@link #get()} returns the state after every change applied so far in this invocation, so the
 * read above already reflects the increment above it.
 *
 * <h2>Why not assign the field directly</h2>
 *
 * <p>State is persisted as the ordered log of applied changes and rebuilt by replaying that log
 * over a snapshot. A field written any other way is absent from the log and therefore absent from
 * the rebuilt object:
 *
 * <pre>{@code
 * state.get().count++;                 // compiles, runs, and is LOST on restart
 * state.apply(new Increment(...));     // the supported form
 * }</pre>
 *
 * <p><b>This is enforced by convention rather than by the type system.</b> {@link #get()} has to
 * return a readable object, so the compiler cannot stop the first line. The Rust SDK forecloses it
 * by implementing {@code Deref} and deliberately not {@code DerefMut}; Java has no equivalent.
 *
 * <p>The failure is silent under normal operation, because a direct assignment behaves correctly
 * for as long as the process survives. What catches it instead is
 * {@code ProcedureTestHarness.replayEquals()}, which replays the recorded changes and compares the
 * result against the observed state. It is opt-in and belongs in CI.
 */
public final class StateHandle<State, ChangeT extends Change> {

    private final State state;
    private final List<ChangeT> buffer;
    private final BiConsumer<State, ChangeT> applyChangeFn;

    StateHandle(State state, List<ChangeT> buffer, BiConsumer<State, ChangeT> applyChangeFn) {
        this.state = state;
        this.buffer = buffer;
        this.applyChangeFn = applyChangeFn;
    }

    /**
     * Factory used by the runtime dispatcher, the workflow driver, and the test
     * harness — all in other packages and modules, so this is necessarily
     * {@code public}. Java has no {@code package(crate)} equivalent, so user code
     * is asked not to construct handles but is not structurally prevented from it.
     */
    public static <S, C extends Change> StateHandle<S, C> create(
            S state, List<C> buffer, BiConsumer<S, C> applyChangeFn) {
        return new StateHandle<>(state, buffer, applyChangeFn);
    }

    /**
     * Reads the state, including every change applied so far in this invocation.
     *
     * <p>The object returned is the live state, not a copy, so assigning to one of its fields
     * compiles and runs. Do not: such a write is not recorded and does not survive a restart.
     * Mutate through {@link #apply} instead.
     *
     * @return this entity's current state
     */
    public State get() {
        return state;
    }

    /**
     * Records a change and applies it to the state. This is the only mutation that survives a
     * restart.
     *
     * <p>The change is handed to the procedure's {@code applyChange} to update the state, and is
     * also written to this entity's change log, from which the state is rebuilt after a crash. A
     * read through {@link #get()} immediately afterwards reflects it.
     *
     * @param change the mutation to record and apply
     */
    public void apply(ChangeT change) {
        applyChangeFn.accept(state, change);
        buffer.add(change);
    }
}
