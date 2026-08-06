package com.flezha.sdk.runtime.control;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Handler for a single {@code CommandRequest} variant. Returns the payload
 * bytes for the {@code raw_data} field of the outgoing {@code CommandResponse}
 * (empty byte[] for commands that don't produce a payload). Adds any errors
 * it wants surfaced to the {@code errors} list.
 */
public interface CommandHandler {

    /** Reply payload plus the metadata to stamp on its frame. */
    record Reply(@Nonnull byte[] rawData, @Nonnull Map<String, String> metadata) {}

    @Nonnull
    byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors)
            throws Exception;

    /**
     * Reply payload plus frame metadata. Overridden only by commands whose
     * answer carries more than bytes — {@code EntityState} stamps
     * {@code flz.change_seq} for a single selected Entity. The default forwards
     * to {@link #handle} with no metadata, which is what every other command
     * wants and keeps their frames byte-identical.
     */
    @Nonnull
    default Reply reply(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors)
            throws Exception {
        return new Reply(handle(ctx, request, errors), Map.of());
    }

    /** Whether the SDK should close the control socket after responding (disconnect command). */
    default boolean closeAfterResponse() {
        return false;
    }
}
