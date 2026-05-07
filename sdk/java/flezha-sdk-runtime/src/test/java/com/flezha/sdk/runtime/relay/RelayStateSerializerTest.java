package com.flezha.sdk.runtime.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.shard.Shard;
import com.flezha.sdk.shard.ShardCtx;
import com.flezha.sdk.shard.ShardException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Per-Shard / per-Partition state-blob shape (per
 * {@code relay-mvp-rust/design.md} Decision 10):
 *
 * <pre>
 *   single-mode shard:  {"shardName": &lt;state&gt;}
 *   static-mode shard:  {"shardName": {"shardKey": &lt;state&gt;, ...}}
 * </pre>
 */
class RelayStateSerializerTest {

    @BeforeEach
    void reset() {
        RelayRegistry.reset();
    }

    @Test
    @DisplayName("serialize emits the documented per-Shard / per-Partition shape")
    void serializeShape() throws Exception {
        RelayRegistry.registerRelay(MixedRelay.class);
        RelayInstance instance = newInstance("mixed");

        // Manually mutate the partition states so the test asserts the
        // serializer reads from the live AtomicReference.
        instance.partitions().get(shardId(instance, "a")).get(0).state().set(new ShardAState(1));
        instance.partitions().get(shardId(instance, "b")).get(0).state().set(new ShardBState(1));
        instance.partitions().get(shardId(instance, "b")).get(1).state().set(new ShardBState(2));

        byte[] blob = RelayStateSerializer.serialize(instance);
        @SuppressWarnings("unchecked")
        Map<String, Object> root = new ObjectMapper().readValue(blob, Map.class);

        assertThat(root).containsKey("a").containsKey("b");
        @SuppressWarnings("unchecked")
        Map<String, Object> shardA = (Map<String, Object>) root.get("a");
        assertThat(shardA).containsEntry("n", 1);

        @SuppressWarnings("unchecked")
        Map<String, Object> shardB = (Map<String, Object>) root.get("b");
        assertThat(shardB).containsKey("1m").containsKey("1h");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1m = (Map<String, Object>) shardB.get("1m");
        @SuppressWarnings("unchecked")
        Map<String, Object> b1h = (Map<String, Object>) shardB.get("1h");
        assertThat(b1m).containsEntry("x", 1);
        assertThat(b1h).containsEntry("x", 2);
    }

    @Test
    @DisplayName("deserialize round-trips identical partition states")
    void deserializeRoundTrips() throws Exception {
        RelayRegistry.registerRelay(MixedRelay.class);
        RelayInstance instance = newInstance("mixed");
        instance.partitions().get(shardId(instance, "a")).get(0).state().set(new ShardAState(7));
        instance.partitions().get(shardId(instance, "b")).get(0).state().set(new ShardBState(11));
        instance.partitions().get(shardId(instance, "b")).get(1).state().set(new ShardBState(13));

        byte[] blob = RelayStateSerializer.serialize(instance);

        RelayInstance second = newInstance("mixed");
        RelayStateSerializer.deserialize(second, blob);

        assertThat(second.partitions().get(shardId(second, "a")).get(0).state().get())
                .isEqualTo(new ShardAState(7));
        assertThat(second.partitions().get(shardId(second, "b")).get(0).state().get())
                .isEqualTo(new ShardBState(11));
        assertThat(second.partitions().get(shardId(second, "b")).get(1).state().get())
                .isEqualTo(new ShardBState(13));
    }

    private static RelayInstance newInstance(String factoryId) {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        ComputeEngineContext ctx = new ComputeEngineContext("ns", cfg, 0L);
        ctx.setWorkerPool(Executors.newFixedThreadPool(1));
        RelayTopology topology = RelayClassRegistry.getTopology(factoryId);
        return RelayInstance.create(topology, factoryId + "-instance", ctx, ctx.workerPool());
    }

    private static int shardId(RelayInstance instance, String shardName) {
        return instance.topology().shardNameToId().get(shardName);
    }

    public record ShardAState(int n) {}
    public record ShardBState(int x) {}

    public static final class ShardA extends StatelessTestShard<String, ShardAState, String> {
        @Override public String name() { return "a"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public Class<String> outputType() { return String.class; }
        @Override protected ShardAState initState(Optional<String> shardKey) { return new ShardAState(0); }
        @Override public void run(String input,
                com.flezha.sdk.state.StateHandle<ShardAState, com.flezha.sdk.state.NoChange> state,
                ShardCtx ctx) throws ShardException { }
    }

    public static final class ShardB extends StatelessTestShard<String, ShardBState, String> {
        @Override public String name() { return "b"; }
        @Override public Class<String> inputType() { return String.class; }
        @Override public Class<String> outputType() { return String.class; }
        @Override protected ShardBState initState(Optional<String> shardKey) { return new ShardBState(0); }
        @Override public List<String> shardKeys() { return List.of("1m", "1h"); }
        @Override public boolean partitionForOverridden() { return true; }
        @Override public List<String> partitionFor(String input) { return List.of("1m"); }
        @Override public void run(String input,
                com.flezha.sdk.state.StateHandle<ShardBState, com.flezha.sdk.state.NoChange> state,
                ShardCtx ctx) throws ShardException { }
    }

    public static final class MixedRelay implements Relay {
        @Override public String factoryId() { return "mixed"; }
        @Override public void channels(ChannelBuilder c) {
            c.input(String.class, "input.a").into(ShardA.class);
            c.input(String.class, "input.b").into(ShardB.class);
        }
    }
}
