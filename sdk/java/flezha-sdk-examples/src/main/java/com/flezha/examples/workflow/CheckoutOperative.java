package com.flezha.examples.workflow;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.RollbackMode;
import com.flezha.sdk.operative.WorkflowOperative;
import java.time.Duration;
import javax.annotation.Nonnull;

/** Reserve, charge, ship. Any failure unwinds everything already done. */
public final class CheckoutOperative implements WorkflowOperative {

    @Override
    @Nonnull
    public String factoryId() {
        return "checkout";
    }

    /** Undo everything on failure. KEEP_PROGRESS only matters for loops. */
    @Override
    @Nonnull
    public RollbackMode rollbackMode() {
        return RollbackMode.ALL_OR_NOTHING;
    }

    /** A failing step is retried this many times before the whole thing unwinds. */
    @Override
    public int retryAttempts() {
        return 3;
    }

    @Override
    @Nonnull
    public Duration retryBackoff() {
        return Duration.ofMillis(500);
    }

    @Override
    public void procedures(@Nonnull ChannelBuilder c) {
        // The value between two steps is written down at that boundary, which is what lets a
        // workflow that died after charging resume at shipping instead of starting over. Nothing
        // here declares how it is written: the runtime derives that from the channel.
        c.input(String.class, "order")
                .into(Reserve.class)
                .named("reserved")
                .into(Charge.class)
                .named("charged")
                .into(Ship.class)
                .sink("shipped");
    }
}
