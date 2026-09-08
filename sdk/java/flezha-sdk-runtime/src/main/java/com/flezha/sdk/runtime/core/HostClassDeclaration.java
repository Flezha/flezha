package com.flezha.sdk.runtime.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.native_.FlezhaNativeException;
import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.StreamOperativeTopology;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ChannelEntry;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ClassFacts;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.PendingEdge;
import com.flezha.sdk.runtime.operative.builder.StreamOperativeTopologyBuilder.ProcedureEntry;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowSpec;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.LogicalClock;
import com.flezha.sdk.state.NoChange;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Writes the poll core's {@code HostClassDecl} record — what the builder was
 * TOLD, never what it concluded — and submits it to the core's one check. The
 * key set is a cross-SDK contract pinned by
 * {@code test/fixtures/host_class_decl_keys.txt}; the core's serde denies
 * unknown fields and defaults absent ones, so optionals that are absent are
 * omitted entirely.
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

    /**
     * This SDK's own released version, stamped into every durable record it
     * writes. Pinned to {@code sdk/java/pom.xml} by
     * {@code test/sdk_conformance/gate_test.exs}; a source constant rather than a
     * filtered resource, because the shade plugin replaces the jar plugin's
     * output in place, so a version written into the thin jar's manifest is not
     * what the fat jar carries. The core packs it.
     */
    private static final String SDK_VERSION = "0.1.0";

    /** Header format byte for the host's plain change bodies (Kryo = 4). */
    private static final int FORMAT_ID_KRYO = 4;

    /** Declared by a stream class alone; see {@link #forWorkflow(WorkflowSpec)}. */
    private static final List<String> STREAM_TIER_KEYS = List.of(
            "effectively_once",
            "checkpoint_ack",
            "checkpoint_every",
            "checkpoint_outputs",
            "checkpoint_max_delay_ms",
            "correction_window_ms",
            "beyond_window_policy");

    private HostClassDeclaration() {}

    /** Resolve {@code factoryId} in the class registries and encode its record. */
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

    /**
     * The record the class was built from, encoded. Written once at build
     * time, so the bytes the core judged are the bytes it registers.
     */
    @Nonnull
    public static byte[] forStream(@Nonnull StreamOperativeTopology topology) {
        return encode(topology.declaration());
    }

    /**
     * The record a stream builder submits: the class facts as declared, one
     * entry per channel and procedure in id order, and per channel the chains
     * its handles were given. Consumers, channel kinds, sink names and the
     * acknowledgement granularity are NOT here — they are the core's answer.
     */
    @Nonnull
    public static Map<String, Object> forStream(
            @Nonnull String factoryId,
            @Nonnull ClassFacts facts,
            @Nonnull List<ChannelEntry> channels,
            @Nonnull List<ProcedureEntry> procedures,
            @Nonnull int[] capacities) {
        Map<String, Object> decl = base(factoryId);
        // Persistence is what the class's Changes imply: a class whose every
        // Procedure declares NoChange has nothing to flush.
        if (procedures.stream().anyMatch(p -> p.changeType != NoChange.class)) {
            decl.put("persistent", true);
        }
        if (facts.effectivelyOnce()) {
            decl.put("effectively_once", true);
        }
        // Every checkpoint knob rides raw; the core derives the granularity
        // from them, which is the rule six per-SDK helpers each carried.
        if (facts.checkpointAck()) {
            decl.put("checkpoint_ack", true);
        }
        if (facts.checkpointEvery() != null) {
            decl.put("checkpoint_every", facts.checkpointEvery());
        }
        if (facts.checkpointMaxDelayMs() != null) {
            decl.put("checkpoint_outputs", "at_checkpoint");
            decl.put("checkpoint_max_delay_ms", facts.checkpointMaxDelayMs());
        }
        if (facts.correctionWindowMs() != null) {
            decl.put("correction_window_ms", facts.correctionWindowMs());
            decl.put("beyond_window_policy", facts.beyondWindowPolicy().wireName());
        }
        long compactAfterBytes =
                procedures.stream().mapToLong(p -> p.compactAfterBytes).min().orElse(DEFAULT_COMPACT_AFTER_BYTES);
        if (compactAfterBytes != DEFAULT_COMPACT_AFTER_BYTES) {
            decl.put("compact_after_bytes", compactAfterBytes);
        }
        decl.put("channels", channels(channels));
        decl.put("procedures", procedures(procedures, capacities));
        return decl;
    }

    /**
     * Submit a record to {@code flz_core}'s one check. Answers the derived
     * declaration, or throws the verdict the core rendered — the names and
     * sentences every SDK reports a refusal under.
     */
    @Nonnull
    public static DerivedDeclaration check(@Nonnull Map<String, Object> record) {
        long ctx = FlzCore.newContext();
        try {
            return DerivedDeclaration.of(JSON.readTree(FlzCore.checkClass(ctx, encode(record))));
        } catch (FlezhaNativeException refusal) {
            throw new StreamOperativeConfigurationException(refusal.getMessage(), refusals(refusal.body()));
        } catch (Exception e) {
            throw new IllegalStateException("the core's derived declaration did not decode: " + e.getMessage(), e);
        } finally {
            FlzCore.contextFree(ctx);
        }
    }

    /**
     * The names the core listed in the verdict it handed out beside the
     * refusal. Empty for a refusal that carried no verdict — a call the JNI
     * layer could not even submit, which the core never named.
     */
    @Nonnull
    private static List<String> refusals(@Nullable byte[] verdict) {
        if (verdict == null || verdict.length == 0) {
            return List.of();
        }
        List<String> names = new ArrayList<>();
        try {
            for (JsonNode issue : JSON.readTree(verdict).path("issues")) {
                names.add(issue.path("name").asText());
            }
        } catch (IOException e) {
            throw new IllegalStateException("the core's verdict did not decode: " + e.getMessage(), e);
        }
        return names;
    }

    @Nonnull
    public static byte[] forWorkflow(@Nonnull WorkflowSpec spec) {
        StreamOperativeTopology topology = spec.topology().orElse(null);
        Map<String, Object> decl =
                topology != null ? new LinkedHashMap<>(topology.declaration()) : emptyRecord(spec.factoryId());
        // Durability is the workflow's own declaration, not what its
        // procedures' Change types imply.
        decl.remove("persistent");
        // The tier, checkpoint and correction keys are a stream class's to put
        // on the wire. The core refuses effectively_once and
        // correction_window_ms on a workflow outright, and the checkpoint
        // family rests on an acknowledgement that refusal leaves unreachable
        // there. The topology keeps the facts either way: gating the
        // declaration must not gate the in-process tier.
        STREAM_TIER_KEYS.forEach(decl::remove);
        if (spec.persistenceEnabled()) {
            decl.put("persistent", true);
        }
        if (spec.syncDurability()) {
            decl.put("sync_durability", true);
        }

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

    private static Map<String, Object> base(String factoryId) {
        Map<String, Object> decl = new LinkedHashMap<>();
        decl.put("factory_id", factoryId);
        decl.put("sdk_lang", SDK_LANG_JAVA);
        decl.put("sdk_version", SDK_VERSION);
        decl.put("change_format_id", FORMAT_ID_KRYO);
        return decl;
    }

    /** A class that declared no channels and no procedures: the run-sugar shape. */
    private static Map<String, Object> emptyRecord(String factoryId) {
        Map<String, Object> decl = base(factoryId);
        decl.put("channels", List.of());
        decl.put("procedures", List.of());
        return decl;
    }

    private static List<Map<String, Object>> channels(List<ChannelEntry> channels) {
        List<Map<String, Object>> out = new ArrayList<>(channels.size());
        for (ChannelEntry channel : channels) {
            Map<String, Object> c = new LinkedHashMap<>();
            c.put("name", channel.name);
            c.put("kind", kindOf(channel.kind));
            if (channel.producer != null) {
                c.put("producer", channel.producer);
            }
            c.put("chains", chains(channel));
            c.put("retry_max", channel.retryMaxAttempts);
            c.put("retry_backoff_ms", channel.retryBackoffBaseMs);
            c.put(
                    "overflow",
                    switch (channel.overflowPolicy) {
                        case DROP_NOTHING -> "drop_nothing";
                        case DROP_NEWEST -> "drop_newest";
                        case DROP_OLDEST -> "drop_oldest";
                    });
            if (channel.keyed) {
                c.put("keyed", true);
            }
            out.add(c);
        }
        return out;
    }

    private static List<List<Map<String, Object>>> chains(ChannelEntry channel) {
        List<List<Map<String, Object>>> out = new ArrayList<>(channel.chains.size());
        for (PendingEdge chain : channel.chains) {
            List<Map<String, Object>> links = new ArrayList<>(chain.links.size());
            for (HostLink link : chain.links) {
                links.add(link.json());
            }
            out.add(links);
        }
        return out;
    }

    private static List<Map<String, Object>> procedures(List<ProcedureEntry> procedures, int[] capacities) {
        List<Map<String, Object>> out = new ArrayList<>(procedures.size());
        for (int i = 0; i < procedures.size(); i++) {
            ProcedureEntry desc = procedures.get(i);
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("name", desc.name);
            if (desc.dynamic) {
                p.put("entity_mode", "dynamic");
                desc.entityIdleTtl.ifPresent(ttl -> p.put("idle_ttl_ms", ttl.toMillis()));
                desc.maxEntities.ifPresent(cap -> p.put("max_entities", cap));
            } else if (!desc.entityKeys.isEmpty()) {
                p.put("entity_mode", "static");
                p.put("entity_keys", desc.entityKeys);
            }
            if (desc.entitiesForOverridden) {
                p.put("entities_for_overridden", true);
            }
            if (desc.compensateOverridden) {
                p.put("compensate_overridden", true);
            }
            p.put("clock", clockOf(desc.clockType));
            p.put("mailbox_capacity", capacities[i]);
            // A procedure whose Change is the empty one journals nothing, so a
            // logical clock over it never advances; the core names that.
            if (desc.changeType == NoChange.class) {
                p.put("change_is_nochange", true);
            }
            // Per Procedure, never a class-wide maximum: one Procedure's bump
            // would otherwise re-stamp its siblings' records with a version
            // they never declared.
            p.put("schema_version", desc.schemaVersion);
            p.put("min_readable_schema_version", desc.minReadableSchemaVersion);
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
