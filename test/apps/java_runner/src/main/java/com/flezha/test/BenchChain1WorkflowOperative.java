package com.flezha.test;

import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/** {@code mix bench.workflow --runtime java --procs 1}: a one-proc durable chain. */
public final class BenchChain1WorkflowOperative implements WorkflowOperative {
    @Override
    public String factoryId() {
        return "bench_chain_1";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        BenchSteps.chain(c, 1);
    }
}
