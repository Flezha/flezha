package com.flezha.sdk.testing;

import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.ReplayMismatch;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Test harness for {@link Shard} authors. Drives a Shard through a sequence of
 * inputs without standing up the runtime, recording the Changes
 * {@code state.apply}-ed and the values {@code ctx.emit}-ed. Mirrors the
 * Rust SDK's {@code ShardTestHarness<S>}.
 *
 * <p>{@link #replayEquals()} is the determinism gate: it reconstructs a fresh
 * state from {@code Shard.initState}, replays the recorded Changes, and
 * compares the Kryo-serialized bytes. Equality means the Shard's state is
 * a deterministic function of the Change log — which is the umbrella's
 * correctness contract.
 */
public final class ShardTestHarness<Input, State, Output, ChangeT extends Change> {

    private final Shard<Input, State, Output, ChangeT, ?, ?> shard;
    private State state;
    private final List<ChangeT> recordedChanges = new ArrayList<>();
    private final List<Object> recordedEmits = new ArrayList<>();

    private ShardTestHarness(Shard<Input, State, Output, ChangeT, ?, ?> shard, State initialState) {
        this.shard = shard;
        this.state = initialState;
    }

    /**
     * Static factory. Calls {@code Shard.initState(Optional.empty(), Optional.empty())}
     * to seed the harness with a fresh state. Override via
     * {@link #withInitialState(Object)} or {@link #withPayload(Optional, RestoreInitPayload)}.
     */
    @Nonnull
    public static <I, S, O, C extends Change> ShardTestHarness<I, S, O, C> of(@Nonnull Shard<I, S, O, C, ?, ?> shard) {
        S init = shard.initState(Optional.empty(), Optional.empty());
        return new ShardTestHarness<>(shard, init);
    }

    @Nonnull
    public ShardTestHarness<Input, State, Output, ChangeT> withInitialState(@Nonnull State state) {
        this.state = state;
        return this;
    }

    @Nonnull
    public ShardTestHarness<Input, State, Output, ChangeT> withPayload(
            @Nonnull Optional<String> shardKey, @Nonnull RestoreInitPayload payload) {
        this.state = shard.initState(shardKey, Optional.of(payload));
        return this;
    }

    /** Run the Shard against each input, recording state mutations and emits. */
    @Nonnull
    public ShardTestHarness<Input, State, Output, ChangeT> applyInputs(@Nonnull Iterable<Input> inputs) {
        for (Input input : inputs) {
            StateHandle<State, ChangeT> handle = StateHandle.create(state, recordedChanges, shard::applyChange);
            TestShardCtx ctx = new TestShardCtx(recordedEmits);
            try {
                shard.run(input, handle, ctx);
            } catch (ShardException e) {
                // Test harness is forgiving — record but keep going.
            }
        }
        return this;
    }

    /** Assert that the recorded Change list equals the supplied list. */
    @Nonnull
    public ShardTestHarness<Input, State, Output, ChangeT> expectChanges(@Nonnull List<ChangeT> expected) {
        if (!recordedChanges.equals(expected)) {
            throw new AssertionError("changes mismatch:\n  expected: " + expected + "\n  actual:   " + recordedChanges);
        }
        return this;
    }

    /**
     * Assert that recorded emits of the given type equal the supplied list.
     * Filters {@code recordedEmits} by type so harnesses with multiple emit
     * channel types remain testable.
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public <C> ShardTestHarness<Input, State, Output, ChangeT> expectEmits(
            @Nonnull Class<C> type, @Nonnull List<C> expected) {
        List<C> actual = new ArrayList<>();
        for (Object emit : recordedEmits) {
            if (type.isInstance(emit)) {
                actual.add((C) emit);
            }
        }
        if (!actual.equals(expected)) {
            throw new AssertionError("emits mismatch (" + type.getSimpleName() + "):\n" + "  expected: " + expected
                    + "\n  actual:   " + actual);
        }
        return this;
    }

    /** Assert that the current state equals the supplied state via {@link Object#equals}. */
    @Nonnull
    public ShardTestHarness<Input, State, Output, ChangeT> expectState(@Nonnull State expected) {
        if (!expected.equals(state)) {
            throw new AssertionError("state mismatch:\n  expected: " + expected + "\n  actual:   " + state);
        }
        return this;
    }

    /**
     * Imperative replay: start from {@code initial}, fold the change log
     * via {@code Shard.applyChange}, and return the resulting state. Useful
     * for cross-checks beyond {@link #replayEquals()}.
     */
    @Nonnull
    public State replay(@Nonnull State initial, @Nonnull List<ChangeT> log) {
        State s = initial;
        for (ChangeT c : log) {
            shard.applyChange(s, c);
        }
        return s;
    }

    /**
     * Determinism gate. Builds a fresh state via
     * {@code Shard.initState(Optional.empty(), Optional.empty())},
     * folds the recorded Change log, then compares the Kryo bytes of the
     * harness's current state against the replayed state. Returns
     * {@code Optional.empty()} on byte-equality (deterministic Shard) or
     * {@link ReplayMismatch} carrying the diagnostic bytes otherwise.
     */
    @Nonnull
    public Optional<ReplayMismatch> replayEquals() {
        State fresh = shard.initState(Optional.empty(), Optional.empty());
        for (ChangeT c : recordedChanges) {
            shard.applyChange(fresh, c);
        }
        KryoCodec codec = new KryoCodec(shard.kryoTypes());
        byte[] expected = codec.serialize(state);
        byte[] actual = codec.serialize(fresh);
        if (Arrays.equals(expected, actual)) {
            return Optional.empty();
        }
        return Optional.of(new ReplayMismatch(expected, actual));
    }

    /** Read-only view of the harness state for direct assertions. */
    @Nonnull
    public State state() {
        return state;
    }

    /** Read-only view of the recorded Change log. */
    @Nonnull
    public List<ChangeT> recordedChanges() {
        return List.copyOf(recordedChanges);
    }

    /** Read-only view of the recorded emit log (mixed types). */
    @Nonnull
    public List<Object> recordedEmits() {
        return List.copyOf(recordedEmits);
    }

    /** Internal {@link ShardCtx} that records emits without invoking the wire path. */
    static final class TestShardCtx implements ShardCtx {

        private final List<Object> recorded;

        TestShardCtx(List<Object> recorded) {
            this.recorded = recorded;
        }

        @Override
        public <C> void emit(@Nonnull C value) {
            recorded.add(value);
        }

        @Override
        public <C> void emit(@Nonnull String channel, @Nonnull C value) {
            recorded.add(value);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull C value, @Nonnull Map<String, String> meta) {
            recorded.add(value);
        }

        @Override
        public <C> void emitWithMeta(@Nonnull String channel, @Nonnull C value, @Nonnull Map<String, String> meta) {
            recorded.add(value);
        }
    }

    /** Construct the metadata map idiomatically; lets users inline Map.of(...) calls. */
    @Nonnull
    public static Map<String, String> meta(@Nonnull String... pairs) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            m.put(pairs[i], pairs[i + 1]);
        }
        return m;
    }
}
