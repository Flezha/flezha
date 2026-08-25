package com.flezha.test;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.FlezhaRunner;

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
        define(Flezha.createRunner(namespace)).run();
    }

    /**
     * Declare every fixture class on {@code runner}, in the order the Rust test
     * app declares its twins. Shared with {@code FactoryIdsTest}, so the pinned
     * factory-id set is the set this app actually advertises.
     */
    public static FlezhaRunner define(FlezhaRunner runner) {
        runner.defineStream(DoNothingStreamOperative.class)
                .defineStream(StoreInputStreamOperative.class)
                .defineStream(CompareFilterStreamOperative.class)
                .defineStream(StaticEntityStreamOperative.class)
                .defineStream(EntityStressStreamOperative.class)
                .defineStream(BoundedMailboxDropNothingStreamOperative.class)
                .defineStream(BoundedMailboxDropNewestStreamOperative.class)
                .defineStream(BoundedMailboxDropOldestStreamOperative.class)
                .defineStream(StatefulCounterStreamOperative.class)
                .defineStream(CoalescingCounterStreamOperative.class)
                .defineStream(PersistenceOffStreamOperative.class)
                .defineStream(HybridChannelStreamOperative.class)
                .defineStream(ResumeProbeStreamOperative.class)
                .defineStream(SchemaEvolutionStreamOperatives.V1.class)
                .defineStream(SchemaEvolutionStreamOperatives.V2.class)
                .defineStream(DynamicEntityStreamOperative.class)
                .defineStream(PartitionRelayStreamOperative.class)
                .defineStream(SessionTimeoutStreamOperative.class)
                .defineStream(MixedStreamOperative.class)
                .defineStream(TierStoreStreamOperative.class)
                .defineStream(BenchEchoStreamOperative.class)
                .defineStream(BenchKeyedStreamOperative.class)
                .defineStream(BenchKeyedNoPersistStreamOperative.class)
                .defineStream(CheckpointStoreStreamOperative.class)
                .defineStream(CheckpointBatchedStoreStreamOperative.class)
                .defineStream(BenchCheckpointStreamOperative.class)
                .defineStream(PoisonProbeStreamOperative.class)
                .defineStream(WedgeProbeStreamOperative.class)
                .defineStream(CandleStreamOperative.class)
                .defineWorkflow(EchoWorkflowOperative.class)
                .defineWorkflow(CommitBlockWorkflowOperative.class)
                .defineWorkflow(SyncDurableWorkflowOperative.class)
                .defineWorkflow(OptOutCommitBlockWorkflowOperative.class)
                .defineWorkflow(TierCommitBlockWorkflowOperative.class)
                .defineWorkflow(SagaBlockWorkflowOperative.class)
                .defineWorkflow(OutboxBlockWorkflowOperative.class)
                .defineWorkflow(UndoOutboxWorkflowOperative.class)
                .defineWorkflow(ChatLoopWorkflowOperative.class)
                .defineWorkflow(StatelessLoopWorkflowOperative.class)
                .defineWorkflow(ChatRewindWorkflowOperative.class)
                .defineWorkflow(RetryWorkflowOperative.class)
                .defineWorkflow(BranchWorkflowOperative.class)
                .defineWorkflow(DiamondWorkflowOperative.class)
                .defineWorkflow(BenchChain1WorkflowOperative.class)
                .defineWorkflow(BenchChain4WorkflowOperative.class)
                .defineWorkflow(BenchChain16WorkflowOperative.class)
                .defineWorkflow(BenchChainOptOut4WorkflowOperative.class)
                .defineWorkflow(BenchLoopWorkflowOperative.class)
                // The authoring-sugar twins. `GeneratedStreamOperatives.defineAll`
                // (asserted by SugarTwinTest) is written in the processor's final
                // round, so a main source cannot resolve it — name the generated
                // operatives, which are written a round earlier.
                .defineStream(SugarDoNothingStreamOperative.class)
                .defineStream(SugarStoreStreamOperative.class)
                .defineStream(SugarStaticEntityStreamOperative.class);
        return runner;
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
