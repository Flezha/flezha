package com.flezha.test;

import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/** {@code mix bench.workflow --runtime java --procs 16}: a sixteen-proc durable chain. */
public final class BenchChain16WorkflowOperative implements WorkflowOperative {
    @Override
    public String factoryId() {
        return "bench_chain_16";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        BenchSteps.chain(c, 16);
    }
}
