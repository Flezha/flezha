package com.flezha.examples.echo;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * A processing stage of the operative's graph, invoked once per message. This implementation
 * prefixes the input and emits the result.
 *
 * <p>The six type arguments are, in declaration order: the input value, the per-entity state, the
 * emitted output, the change type used to record state mutations, the clock, and the configuration
 * type. Two are opted out of here. {@link NoChange} declares that this procedure persists nothing,
 * which disables the persistence machinery entirely, and {@link Void} declares that it takes no
 * configuration. The runtime recovers all six as {@code Class} handles from the {@code implements}
 * clause, so each must be concrete there.
 *
 * <p>Invocation is serialised per entity: two messages carrying the same key are never handled
 * concurrently, so no internal locking is required around per-entity state. Messages for distinct
 * keys are handled concurrently, on this host or another.
 */
public final class EchoProcedure implements Procedure<String, EchoProcedure.None, String, NoChange, WallClock, Void> {

    /**
     * The per-entity state type. This procedure retains nothing between messages, so the record has
     * no components. The {@code state} example covers what changes once state is required.
     */
    public record None() {}

    /**
     * The procedure's stable identity. This value is written into durable state, so changing it
     * does not rename an existing procedure: it declares a different one, and state recorded under
     * the previous name is no longer associated with it. Choose it deliberately at authoring time.
     */
    @Override
    @Nonnull
    public String name() {
        return "echo";
    }

    /**
     * Constructs the initial state for one entity, invoked on first sight of a key. The arguments
     * are the entity key, and a payload supplied when the entity is being relocated from another
     * host rather than started cold. On a resume the runtime restores the most recent snapshot and
     * folds the change log over whatever this returns, so it need only be correct for the cold
     * case. Neither argument is required here.
     */
    @Override
    @Nonnull
    public None initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new None();
    }

    /**
     * Invoked once per message. {@link ProcedureCtx} is the runtime handle: it supplies output
     * through {@code emit}, the declared clock through {@code now()}, and a replay-stable
     * identifier through {@code stableId()}.
     *
     * <p>The destination of an emitted value is not declared here. It is determined by the
     * operative, so the graph can be rewired without modifying this class.
     */
    @Override
    public void run(@Nonnull String input, @Nonnull StateHandle<None, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit("echo: " + input);
    }
}
