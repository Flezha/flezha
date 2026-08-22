package com.flezha.test;

import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.channel.ChannelBuilder;

/** {@code mix bench.workflow --runtime java --procs 4}: a four-proc durable chain. */
public final class BenchChain4WorkflowOperative implements WorkflowOperative {
    @Override
    public String factoryId() {
        return "bench_chain_4";
    }

    @Override
    public void procedures(ChannelBuilder c) {
        BenchSteps.chain(c, 4);
    }
}
