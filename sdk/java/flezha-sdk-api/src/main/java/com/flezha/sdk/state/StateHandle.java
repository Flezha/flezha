package com.flezha.sdk.state;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Mutation seam threaded into {@code Procedure.run}. Reads go through
 * {@link #get()}; the recorded, replayable mutation path is {@link #apply(Change)}.
 *
 * <p><b>Enforcement is by convention, not structure.</b> {@link #get()} returns
 * the live State object, so {@code state.get().field = x} compiles and runs — and
 * is never journaled as a Change, so it is lost on any restore (which rebuilds
 * state from a snapshot plus the change log). Rust forecloses this at compile
 * time by implementing {@code Deref} and deliberately not {@code DerefMut}; Java
 * cannot, because {@code get()} must return a readable object. The detection
 * available in place of enforcement is
 * {@code ProcedureTestHarness.replayEquals()} — an opt-in determinism gate meant
 * to run in CI. See {@code docs/java_state_persistence.md}.
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
     * Factory used by the runtime dispatcher, the bounded driver, and the test
     * harness — all in other packages and modules, so this is necessarily
     * {@code public}. Java has no {@code package(crate)} equivalent, so user code
     * is asked not to construct handles but is not structurally prevented from it.
     */
    public static <S, C extends Change> StateHandle<S, C> create(
            S state, List<C> buffer, BiConsumer<S, C> applyChangeFn) {
        return new StateHandle<>(state, buffer, applyChangeFn);
    }

    /**
     * Read access to the underlying state. Idiomatic Java equivalent of Rust's
     * {@code Deref}: the user writes {@code state.get().field} rather than
     * Rust's {@code state.field}.
     */
    public State get() {
        return state;
    }

    /**
     * Mutation chokepoint. Invokes {@code Procedure::applyChange} on the
     * underlying state and pushes the Change onto the per-Entity flush
     * buffer.
     */
    public void apply(ChangeT change) {
        applyChangeFn.accept(state, change);
        buffer.add(change);
    }
}
