package com.flezha.sdk.runtime.operative;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.proto.AddOperativeResponse;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.bounded.BoundedSpec;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The declaration map is the only channel through which the controller learns a
 * class's capabilities ({@code Overwatch.decode_declarations/1} reads it and
 * nothing else), so both halves matter: what a declaring class says, and the
 * fact that a non-declaring class still encodes to zero bytes.
 */
class DeclarationsTest {

    @BeforeEach
    void reset() {
        BoundedOperativeClassRegistry.reset();
    }

    private static AddOperativeResponse decode(byte[] bytes) throws Exception {
        return AddOperativeResponse.parseFrom(bytes);
    }

    @Test
    @DisplayName("a bounded chain declares its compensable ops and its leaf")
    void boundedChainDeclaresCompensableOpsAndLeaf() throws Exception {
        BoundedOperativeClassRegistry.registerClass(MixedSagaOperative.class);
        BoundedSpec spec = BoundedOperativeClassRegistry.getSpec("declarations-mixed-saga");
        assertThat(spec).isNotNull();

        AddOperativeResponse response = decode(Declarations.forBounded(spec));

        // Reserve (0) and Ship (2) declare compensate; Charge (1) does not.
        assertThat(response.getMetadataMap()).containsEntry("compensable_ops", "0,2");
        // A linear chain has exactly one leaf: its last op.
        assertThat(response.getMetadataMap()).containsEntry("leaf_ops", "2");
    }

    @Test
    @DisplayName("a chain with nothing compensable declares an EMPTY value, not a missing key")
    void nothingCompensableDeclaresAnEmptyValue() throws Exception {
        BoundedOperativeClassRegistry.registerClass(NoCompensateOperative.class);
        BoundedSpec spec = BoundedOperativeClassRegistry.getSpec("declarations-no-compensate");

        AddOperativeResponse response = decode(Declarations.forBounded(spec));

        // The controller distinguishes these: a MISSING key means the class
        // declared nothing at all (run-sugar / a non-parity SDK), an EMPTY value
        // means a chain in which nothing is compensable. A rewind treats them
        // differently, so the key must be present.
        assertThat(response.getMetadataMap()).containsKey("compensable_ops");
        assertThat(response.getMetadataMap()).containsEntry("compensable_ops", "");
        assertThat(response.getMetadataMap()).containsEntry("leaf_ops", "0");
    }

    @Test
    @DisplayName("run-sugar declares nothing and encodes to zero bytes")
    void runSugarDeclaresNothing() {
        BoundedOperativeClassRegistry.registerClass(RunSugarOperative.class);
        BoundedSpec spec = BoundedOperativeClassRegistry.getSpec("declarations-run-sugar");
        assertThat(spec.isRunSugar()).isTrue();

        // Byte-identical to a pre-declaration build: the controller reads zero
        // bytes as "declared nothing".
        assertThat(Declarations.forBounded(spec)).isEmpty();
    }

    // --- fixtures -----------------------------------------------------------

    /** Reserve (compensable) → Charge (not) → Ship (compensable). */
    public static final class MixedSagaOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "declarations-mixed-saga";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(Reserve.class);
            c.channel(String.class, "c1").from(Reserve.class).codec().into(Charge.class);
            c.channel(String.class, "c2").from(Charge.class).codec().into(Ship.class);
            c.channel(String.class, "out").from(Ship.class).toPlugin("sink");
        }
    }

    public static final class NoCompensateOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "declarations-no-compensate";
        }

        @Override
        public void operations(ChannelBuilder c) {
            c.input(String.class, "in").into(Charge.class);
            c.channel(String.class, "out").from(Charge.class).toPlugin("sink");
        }
    }

    public static final class RunSugarOperative implements BoundedOperative {
        @Override
        public String factoryId() {
            return "declarations-run-sugar";
        }

        @Override
        public void run(com.flezha.sdk.bounded.BoundedOperativeCtx ctx, String args) {}
    }

    public record Unit() {}

    /** Minimal String→String op; subclasses vary only in name and compensate. */
    abstract static class StrOp implements Operation<String, Unit, String, NoChange, WallClock, Void> {
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Unit initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Unit();
        }

        @Override
        public void run(String input, StateHandle<Unit, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit(input);
        }
    }

    public static final class Reserve extends StrOp {
        @Override
        public String name() {
            return "declarations_reserve";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {}
    }

    /** Deliberately declares NO compensate — this is what makes the map "0,2". */
    public static final class Charge extends StrOp {
        @Override
        public String name() {
            return "declarations_charge";
        }
    }

    public static final class Ship extends StrOp {
        @Override
        public String name() {
            return "declarations_ship";
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Unit, NoChange> state, OperationCtx ctx) {}
    }
}
