package com.flezha.sdk.state;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Mutation chokepoint threaded into {@code Operation.run}. Reads go through
 * {@link #get()}; mutations go through {@link #apply(Change)}. There is no
 * setter — to mutate state outside {@code apply}, the user must declare a
 * Change type. That's the structural enforcement.
 *
 * <p>Constructed by the runtime; the package-private constructor forecloses
 * user instantiation outside the {@code com.flezha.sdk.state} package and the
 * test harness package, both of which the runtime privileges.
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
     * Package-private factory used by the runtime and the test harness — both
     * sit inside the same {@code com.flezha.sdk.state} package via
     * trampoline classes that re-export this constructor. Java has no
     * {@code package(crate)} equivalent; this is the closest analog.
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
     * Mutation chokepoint. Invokes {@code Operation::applyChange} on the
     * underlying state and pushes the Change onto the per-Segment flush
     * buffer.
     */
    public void apply(ChangeT change) {
        applyChangeFn.accept(state, change);
        buffer.add(change);
    }
}
