package com.flezha.examples.workflow;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/** What every step here shares: a string in, a string out, and an undo handle in state. */
abstract class Step implements Procedure<String, Step.Handle, String, NoChange, WallClock, Void> {

    /** Restored on resume, unlike emitted output, so this is where an undo handle belongs. */
    public static final class Handle {
        public String reservationId;
    }

    @Override
    @Nonnull
    public Handle initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new Handle();
    }
}
