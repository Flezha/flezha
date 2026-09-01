package com.flezha.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the factory ids this app advertises to the set {@code test/apps/rust_runner}
 * advertises — the cross-SDK fixture contract. A Java fixture without a Rust twin
 * (or a twin under a different id) fails here rather than at an e2e that silently
 * skips the Java arm.
 */
class FactoryIdsTest {

    /** The Rust test app's live factory set, verbatim. */
    private static final Set<String> RUST_FACTORY_IDS = Set.of(
            "bench_chain_1",
            "bench_chain_16",
            "bench_chain_4",
            "bench_chain_optout_4",
            "bench_checkpoint",
            "bench_echo",
            "bench_keyed",
            "bench_keyed_nopersist",
            "bench_loop_workflow",
            "bounded_mailbox_drop_nothing",
            "bounded_mailbox_drop_newest",
            "bounded_mailbox_drop_oldest",
            "branch_workflow",
            "candle",
            "chat_loop_workflow",
            "chat_rewind_workflow",
            "checkpoint_batched_store",
            "checkpoint_store",
            "coalescing_counter",
            "commit_block_workflow",
            "compare_filter",
            "diamond_workflow",
            "do_nothing",
            "dynamic_entity",
            "echo_workflow",
            "entity_stress",
            "entry_idle_probe",
            "hybrid_channel",
            "mixed_operative",
            "optout_commit_block",
            "outbox_block_workflow",
            "partition_relay",
            "persistence_off_counter",
            "poison_probe",
            "resume_probe",
            "retry_workflow",
            "saga_block_workflow",
            "schema_evolution_v1",
            "schema_evolution_v2",
            "session_timeout",
            "stateful_counter",
            "stateless_loop_workflow",
            "static_entity",
            "store_input",
            "sugar_do_nothing",
            "sugar_static_entity",
            "sugar_store_input",
            "sync_durable_workflow",
            "tier_commit_block_workflow",
            "tier_store",
            "undo_outbox_workflow",
            "wedge_probe");

    @BeforeEach
    void reset() {
        StreamOperativeClassRegistry.reset();
        WorkflowOperativeClassRegistry.reset();
    }

    @Test
    @DisplayName("every factory matches the Rust test app's live set")
    void sameIds() {
        Main.define(Flezha.createRunner("factory_ids_ns"));

        Set<String> live = new HashSet<>(StreamOperativeClassRegistry.factoryIds());
        live.addAll(WorkflowOperativeClassRegistry.factoryIds());
        assertEquals(RUST_FACTORY_IDS, live);
    }
}
