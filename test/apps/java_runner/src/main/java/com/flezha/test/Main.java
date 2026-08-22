package com.flezha.test;

import com.flezha.sdk.Flezha;

/**
 * Mirror of {@code test/apps/rust_runner/src/main.rs}. Accepts the
 * namespace as its first command-line argument; {@code run()} keeps the
 * process alive until shutdown, and a watcher thread exits on stdin EOF
 * because the Elixir e2e suite manages this app as an Erlang Port and
 * terminates it by closing the port (= our stdin).
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String namespace = args.length > 0 ? args[0] : "default";
        spawnStdinEofExit();
        Flezha.createRunner(namespace)
                .defineStream(DoNothingStreamOperative.class)
                .defineStream(StoreInputStreamOperative.class)
                .defineStream(CompareFilterStreamOperative.class)
                .defineStream(StatefulCounterStreamOperative.class)
                .defineStream(CoalescingCounterStreamOperative.class)
                .defineStream(PersistenceOffStreamOperative.class)
                .defineStream(HybridChannelStreamOperative.class)
                .defineStream(PoisonProbeStreamOperative.class)
                .defineStream(DynamicEntityStreamOperative.class)
                .defineStream(SessionTimeoutStreamOperative.class)
                // EchoMessageStreamOperative is reintroduced once per-Procedure `handleMessage` lands.
                .defineWorkflow(EchoWorkflowOperative.class)
                .defineWorkflow(CommitBlockWorkflowOperative.class)
                .defineWorkflow(SagaBlockWorkflowOperative.class)
                .defineWorkflow(DiamondWorkflowOperative.class)
                .defineWorkflow(BenchChain1WorkflowOperative.class)
                .defineWorkflow(BenchChain4WorkflowOperative.class)
                .defineWorkflow(BenchChain16WorkflowOperative.class)
                .defineWorkflow(BenchChainOptOut4WorkflowOperative.class)
                .defineWorkflow(BenchLoopWorkflowOperative.class)
                .run();
    }

    private static void spawnStdinEofExit() {
        Thread watcher = new Thread(() -> {
            try {
                byte[] buf = new byte[64];
                while (System.in.read(buf) != -1) {
                    // Discard; only EOF matters.
                }
            } catch (Exception ignored) {
                // A broken stdin counts as EOF.
            }
            System.exit(0);
        }, "stdin-eof-exit");
        watcher.setDaemon(true);
        watcher.start();
    }
}
