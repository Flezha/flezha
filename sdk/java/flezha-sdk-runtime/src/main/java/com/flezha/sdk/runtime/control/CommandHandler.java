package com.flezha.sdk.runtime.control;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ComputeEngineContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Handler for a single {@code CommandRequest} variant. Returns the payload
 * bytes for the {@code raw_data} field of the outgoing {@code CommandResponse}
 * (empty byte[] for commands that don't produce a payload). Adds any errors
 * it wants surfaced to the {@code errors} list.
 */
public interface CommandHandler {

    @Nonnull
    byte[] handle(
            @Nonnull ComputeEngineContext ctx,
            @Nonnull CommandRequest request,
            @Nonnull List<String> errors) throws Exception;

    /** Whether the SDK should close the control socket after responding (disconnect command). */
    default boolean closeAfterResponse() {
        return false;
    }
}
