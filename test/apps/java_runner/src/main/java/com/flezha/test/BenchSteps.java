package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.procedure.Procedure;
import java.util.List;

/**
 * Step classes for the {@code mix bench.workflow} Java fixtures — the mirror of
 * the Rust test app's {@code BenchS0..BenchS15} and {@code BenchTurn}. Each
 * chain position needs a distinct {@link Procedure}
 * class, so the durable pass-through steps are enumerated; every one is the
 * plain {@link DurableStep} (one idempotent {@code Insert} per run, input
 * passed on), differing only in {@link #name()}. Public static nested classes
 * so the SDK's topology builder can reflectively instantiate them.
 */
public final class BenchSteps {
    private BenchSteps() {}

    // The durable pass-through chain, indexed by proc position. All steps are
    // String -> String, so the whole chain keeps one Stream<String> type.
    private static final List<Class<? extends Procedure<String, ?, String, ?, ?, ?>>> CHAIN =
            List.of(
                    S0.class, S1.class, S2.class, S3.class, S4.class, S5.class, S6.class, S7.class,
                    S8.class, S9.class, S10.class, S11.class, S12.class, S13.class, S14.class,
                    S15.class);

    /** Build {@code in -> S0 -> ... -> S{procs-1} -> bench_sink} on {@code c}. */
    public static void chain(ChannelBuilder c, int procs) {
        Stream<String> s = c.input(String.class, "in").into(CHAIN.get(0));
        for (int i = 1; i < procs; i++) {
            s = s.named("edge_BenchS" + i).into(CHAIN.get(i));
        }
        s.named("out").sink("bench_sink");
    }

    public static final class S0 extends DurableStep { public String name() { return "bench_s0"; } }
    public static final class S1 extends DurableStep { public String name() { return "bench_s1"; } }
    public static final class S2 extends DurableStep { public String name() { return "bench_s2"; } }
    public static final class S3 extends DurableStep { public String name() { return "bench_s3"; } }
    public static final class S4 extends DurableStep { public String name() { return "bench_s4"; } }
    public static final class S5 extends DurableStep { public String name() { return "bench_s5"; } }
    public static final class S6 extends DurableStep { public String name() { return "bench_s6"; } }
    public static final class S7 extends DurableStep { public String name() { return "bench_s7"; } }
    public static final class S8 extends DurableStep { public String name() { return "bench_s8"; } }
    public static final class S9 extends DurableStep { public String name() { return "bench_s9"; } }
    public static final class S10 extends DurableStep { public String name() { return "bench_s10"; } }
    public static final class S11 extends DurableStep { public String name() { return "bench_s11"; } }
    public static final class S12 extends DurableStep { public String name() { return "bench_s12"; } }
    public static final class S13 extends DurableStep { public String name() { return "bench_s13"; } }
    public static final class S14 extends DurableStep { public String name() { return "bench_s14"; } }
    public static final class S15 extends DurableStep { public String name() { return "bench_s15"; } }

    /** The loop's durable first step: bounded state, input passed on. */
    public static final class Turn extends DurableStep { public String name() { return "bench_turn"; } }
}
