package com.flezha.test;

import com.flezha.sdk.FlezhaRunner;
import com.flezha.sdk.IdleObserver;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mirror of the Rust test app's {@code EntryIdleProbeOperative} (entry-idle
 * e2e).
 *
 * <p>Two entry channels fanning into one procedure, and the split is
 * load-bearing. The core interns one
 * observation cell per channel <b>name</b> — one name is one observed input
 * however many classes declare it — so a frame on {@code idle_report} leaves
 * {@code idle_input}'s stamp untouched, and the e2e can poll for an episode
 * without ending the silence it is waiting for. That same rule is why the
 * watched channel is not called {@code input}, which twenty-five operatives in
 * this app declare.
 */
public final class EntryIdleProbeStreamOperative implements StreamOperative {

    static final String WATCHED = "idle_input";

    /**
     * The tick is a quarter of this, clamped to {@code [250 ms, 1 s]}, so an
     * episode is reported within 250 ms of the threshold passing.
     */
    static final Duration THRESHOLD = Duration.ofMillis(1_000);

    static final AtomicInteger EPISODES = new AtomicInteger();
    static final AtomicInteger RESUMES = new AtomicInteger();
    static final AtomicBoolean ABSENT_AT_BOOT = new AtomicBoolean();

    /**
     * Read the accessor before {@code run()}, then watch the channel. The stamp
     * is taken ahead of anything that can decline a frame, so "absent before the
     * first frame" is observable here and nowhere inside a procedure.
     *
     * <p>Called from {@code Main.main} and deliberately not from
     * {@code Main.define}: a runner-level registration is not a declaration, and
     * {@code define} is shared with {@code FactoryIdsTest}.
     */
    public static void arm(FlezhaRunner runner) {
        ABSENT_AT_BOOT.set(runner.lastEntryDelivery(WATCHED).isEmpty());
        runner.onEntryIdle(WATCHED, THRESHOLD, new IdleObserver() {
            @Override
            public void idle(Duration gap) {
                EPISODES.incrementAndGet();
            }

            @Override
            public void resumed(Duration gap) {
                RESUMES.incrementAndGet();
            }
        });
    }

    @Override
    public String factoryId() {
        return "entry_idle_probe";
    }

    @Override
    public void channels(ChannelBuilder c) {
        // Both channels wire the same class: one node, fan-in.
        c.input(String.class, WATCHED).into(EntryIdleProbeProcedure.class);
        var sReport = c.input(String.class, "idle_report").into(EntryIdleProbeProcedure.class);
        sReport.named("report").sink("entry_idle_report");
    }
}
