package com.flezha.sdk.runtime.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ProcedureDescriptor;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowSpec;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.LogicalClock;
import com.flezha.sdk.state.NoChange;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Builds the poll core's {@code HostClassDecl} JSON from a registered class —
 * the body of {@code FlzCore.declareClass}. The core's serde denies unknown
 * fields and defaults absent ones, so optionals that are absent are omitted
 * entirely.
 *
 * <p><b>Id order is the contract:</b> {@code channels} is emitted in Java's
 * channel-id order and {@code procedures} in procedure-id order, so the core's
 * indices equal Java's — {@code InputItem.channel} and
 * {@code Output.target.channel} are these indexes.
 */
public final class HostClassDeclaration {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** The SDK/controller-aligned default; emitting it would be redundant. */
    private static final long DEFAULT_COMPACT_AFTER_BYTES = 8L * 1024 * 1024;

    /** Header lang byte for durable records (Rust = 1, Java = 2). */
    private static final int SDK_LANG_JAVA = 2;

    /** Header format byte for the host's plain change bodies (Kryo = 4). */
    private static final int FORMAT_ID_KRYO = 4;

    private HostClassDeclaration() {}

    /** Resolve {@code factoryId} in the class registries and build its declaration. */
    @Nonnull
    public static byte[] forFactory(@Nonnull String factoryId) {
        StreamOperativeTopology topology = StreamOperativeClassRegistry.getTopology(factoryId);
        if (topology != null) {
            return forStream(topology);
        }
        WorkflowSpec spec = WorkflowOperativeClassRegistry.getSpec(factoryId);
        if (spec != null) {
            return forWorkflow(spec);
        }
        throw new IllegalStateException("factory `" + factoryId + "` is not registered");
    }

    @Nonnull
    public static byte[] forStream(@Nonnull StreamOperativeTopology topology) {
        Map<String, Object> decl = base(topology.factoryId(), topology);
        // Persistence is what the class's Changes imply: a class whose every
        // Procedure declares NoChange has nothing to flush.
        if (topology.procedures().stream().anyMatch(p -> p.changeType() != NoChange.class)) {
            decl.put("persistent", true);
        }
        if (topology.effectivelyOnce()) {
            decl.put("effectively_once", true);
        }
        if (topology.checkpointAck()) {
            decl.put("ack_granularity", "checkpoint");
            // Every key is omitted at its default: the core deserializes the
            // declaration with `deny_unknown_fields`, so a class that declares
            // nothing new stays byte-identical against an older core.
            var checkpoint = topology.checkpoint();
            if (checkpoint != null && checkpoint.every() > 0) {
                decl.put("checkpoint_every", checkpoint.every());
            }
            if (checkpoint != null && checkpoint.outputsAtCheckpoint()) {
                decl.put("checkpoint_outputs", "at_checkpoint");
                decl.put("checkpoint_max_delay_ms", checkpoint.maxDelayMs());
            }
        }
        if (topology.correctionWindowMs() > 0) {
            decl.put("correction_window_ms", topology.correctionWindowMs());
            decl.put("beyond_window_policy", topology.beyondWindowPolicy().wireName());
        }
        long compactAfterBytes = topology.procedures().stream()
                .mapToLong(ProcedureDescriptor::compactAfterBytes)
                .min()
                .orElse(DEFAULT_COMPACT_AFTER_BYTES);
        if (compactAfterBytes != DEFAULT_COMPACT_AFTER_BYTES) {
            decl.put("compact_after_bytes", compactAfterBytes);
        }
        decl.put("channels", channels(topology));
        decl.put("procedures", procedures(topology));
        return encode(decl);
    }

    @Nonnull
    public static byte[] forWorkflow(@Nonnull WorkflowSpec spec) {
        StreamOperativeTopology topology = spec.topology().orElse(null);
        Map<String, Object> decl =
                base(spec.factoryId(), topology != null ? topology : emptyTopology(spec.factoryId()));
        if (spec.persistenceEnabled()) {
            decl.put("persistent", true);
        }
        if (spec.syncDurability()) {
            decl.put("sync_durability", true);
        }
        decl.put("channels", topology == null ? List.of() : channels(topology));
        decl.put("procedures", topology == null ? List.of() : procedures(topology));

        Map<String, Object> workflow = new LinkedHashMap<>();
        workflow.put(
                "loop_mode",
                switch (spec.loopMode()) {
                    case ONCE -> "once";
                    case PER_INPUT -> "per_input";
                    case CHAINED -> "chained";
                });
        workflow.put(
                "rollback_mode",
                switch (spec.rollbackMode()) {
                    case ALL_OR_NOTHING -> "all_or_nothing";
                    case KEEP_PROGRESS -> "keep_progress";
                });
        workflow.put("retry_attempts", spec.retry().maxAttempts());
        workflow.put("retry_backoff_ms", spec.retry().backoffBaseMs());
        workflow.put("barrier_retry_attempts", spec.barrierRetryAttempts());
        spec.maxDuration().ifPresent(d -> workflow.put("max_duration_ms", d.toMillis()));
        spec.wakeAfter().ifPresent(d -> workflow.put("wake_after_ms", d.toMillis()));
        decl.put("workflow", workflow);
        return encode(decl);
    }

    private static Map<String, Object> base(String factoryId, StreamOperativeTopology topology) {
        Map<String, Object> decl = new LinkedHashMap<>();
        decl.put("factory_id", factoryId);
        int schemaVersion = topology.procedures().stream()
                .mapToInt(ProcedureDescriptor::schemaVersion)
                .max()
                .orElse(1);
        decl.put("schema_version", schemaVersion);
        decl.put("sdk_lang", SDK_LANG_JAVA);
        decl.put("change_format_id", FORMAT_ID_KRYO);
        return decl;
    }

    private static StreamOperativeTopology emptyTopology(String factoryId) {
        return new StreamOperativeTopology(
                factoryId,
                List.of(),
                Map.of(),
                List.of(),
                Map.of(),
                false,
                null,
                0,
                com.flezha.sdk.channel.BeyondWindowPolicy.PROCESS_AS_ARRIVAL);
    }

    private static List<Map<String, Object>> channels(StreamOperativeTopology topology) {
        List<Map<String, Object>> out = new ArrayList<>(topology.channels().size());
        for (ChannelDescriptor channel : topology.channels()) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("name", channel.name());
            c.put("kind", kindOf(channel.kind()));
            channel.producer().ifPresent(p -> c.put("producer", p));
            c.put("consumers", channel.consumerIds());
            c.put("retry_max", channel.retry().maxAttempts());
            c.put("retry_backoff_ms", channel.retry().backoffBaseMs());
            c.put(
                    "overflow",
                    switch (channel.overflowPolicy()) {
                        case BLOCK -> "block";
                        case DROP_NEWEST -> "drop_newest";
                        case DROP_OLDEST -> "drop_oldest";
                    });
            if (channel.keyed()) {
                c.put("keyed", true);
            }
            out.add(c);
        }
        return out;
    }

    private static List<Map<String, Object>> procedures(StreamOperativeTopology topology) {
        List<Map<String, Object>> out = new ArrayList<>(topology.procedures().size());
        for (ProcedureDescriptor desc : topology.procedures()) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("name", desc.name());
            if (desc.dynamic()) {
                p.put("entity_mode", "dynamic");
                desc.entityIdleTtl().ifPresent(ttl -> p.put("idle_ttl_ms", ttl.toMillis()));
                desc.maxEntities().ifPresent(cap -> p.put("max_entities", cap));
            } else if (!desc.entityKeys().isEmpty()) {
                p.put("entity_mode", "static");
                p.put("entity_keys", desc.entityKeys());
            }
            if (desc.entitiesForOverridden()) {
                p.put("entities_for_overridden", true);
            }
            if (desc.compensateOverridden()) {
                p.put("compensate_overridden", true);
            }
            p.put("clock", clockOf(desc.clockType()));
            p.put("mailbox_capacity", desc.mailboxCapacity());
            out.add(p);
        }
        return out;
    }

    private static String kindOf(ChannelKind kind) {
        return switch (kind) {
            case ENTRY -> "entry";
            case INTER_PROCEDURE -> "inter_proc";
            case PLUGIN_BOUND -> "plugin";
        };
    }

    private static String clockOf(Class<?> clockType) {
        if (clockType == EventTimeFrom.class) {
            return "event_time";
        }
        if (clockType == LogicalClock.class) {
            return "logical";
        }
        return "wall";
    }

    private static byte[] encode(Map<String, Object> decl) {
        try {
            return JSON.writeValueAsBytes(decl);
        } catch (Exception e) {
            throw new IllegalStateException("class declaration encoding failed: " + e.getMessage(), e);
        }
    }
}
