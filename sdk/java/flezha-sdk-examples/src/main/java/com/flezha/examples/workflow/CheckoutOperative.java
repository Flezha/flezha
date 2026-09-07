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
        // codec() on every handoff, and it is required here rather than optional. The value between
        // two steps is written down at that boundary, which is what lets a workflow that died after
        // charging resume at shipping instead of starting over.
        //
        // A plain stream operative does not need it, because nothing is journaled between stages
        // there.
        c.input(String.class, "order")
                .into(Reserve.class)
                .named("reserved")
                .codec()
                .into(Charge.class)
                .named("charged")
                .codec()
                .into(Ship.class)
                .named("shipped")
                .sink("orders_sink");
    }
}
