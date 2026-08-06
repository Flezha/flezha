package com.flezha.sdk.runtime.operative;

import com.flezha.sdk.proto.AddOperativeResponse;
import com.flezha.sdk.runtime.bounded.BoundedSpec;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

/**
 * The SDK's capability declaration, encoded as {@code AddOperativeResponse}
 * metadata on the {@code AddOperative} reply.
 *
 * <p>This is the <b>only</b> channel through which the controller learns what an
 * SDK class supports: {@code Overwatch.decode_declarations/1} reads this map and
 * nothing else. A capability the controller must gate on — the effectively-once
 * tier, checkpoint ack granularity, a correction window, which bounded procs are
 * compensable, which are leaves — is invisible until it appears here.
 *
 * <p>A class that declares nothing encodes to <b>zero bytes</b>, which the
 * controller reads as "declared nothing". That keeps the reply byte-identical to
 * a pre-declaration build, so adding this map changes nothing for classes that
 * do not opt in.
 *
 * <p>Mirrors the Rust SDK's {@code command::operative_start::metadata_response}.
 */
public final class Declarations {

    /** The SDK/controller-aligned default; emitting it would be redundant. */
    private static final long DEFAULT_COMPACT_AFTER_BYTES = 8L * 1024 * 1024;

    private Declarations() {}

    /** Encode an unbounded class's declarations. Empty bytes when it declares nothing. */
    @Nonnull
    public static byte[] forUnbounded(@Nonnull UnboundedOperativeTopology topology) {
        Map<String, String> metadata = new LinkedHashMap<>();
        if (topology.effectivelyOnce()) {
            metadata.put("effectively_once", "true");
        }
        // The controller's dataflow-stop watchdog reads "produced output before,
        // silent since" as a dead feed. For a checkpoint class that silence IS
        // the steady state, so it has to know.
        if (topology.checkpointAck()) {
            metadata.put("ack_granularity", "checkpoint");
        }
        // The controller owns the two prerequisites the SDK cannot see — the
        // input-log hold window and the snapshot age floor must both cover this
        // window, or the past it promises to repair is already gone.
        if (topology.correctionWindowMs() > 0) {
            metadata.put("correction_window_ms", String.valueOf(topology.correctionWindowMs()));
            metadata.put("beyond_window_policy", topology.beyondWindowPolicy().wireName());
        }
        // The Watchdog resolves the WAL size trigger per operative_class, so ship
        // the most conservative (smallest) declared threshold across this
        // operative's procedures — an override on any one must not be ignored.
        // Only when it differs from the shared default, keeping an unchanged
        // class's reply byte-identical.
        long compactAfterBytes = topology.procedures().stream()
                .mapToLong(ProcedureDescriptor::compactAfterBytes)
                .min()
                .orElse(DEFAULT_COMPACT_AFTER_BYTES);
        if (compactAfterBytes != DEFAULT_COMPACT_AFTER_BYTES) {
            metadata.put("compact_after_bytes", String.valueOf(compactAfterBytes));
        }
        return encode(metadata);
    }

    /**
     * Encode a bounded class's declarations: which tree positions are
     * compensable, and which are leaves.
     *
     * <p>Run-sugar has no procs, so it declares nothing. Note the distinction the
     * controller depends on: a <b>missing</b> {@code compensable_procs} key means
     * the class declared nothing at all (run-sugar, or a non-parity SDK), while
     * an <b>empty</b> value means a chain in which nothing is compensable — a
     * rewind treats those differently.
     */
    @Nonnull
    public static byte[] forBounded(@Nonnull BoundedSpec spec) {
        Map<String, String> metadata = new LinkedHashMap<>();
        if (spec.isRunSugar() || spec.topology().isEmpty()) {
            return encode(metadata);
        }
        UnboundedOperativeTopology topology = spec.topology().orElseThrow();
        java.util.List<Integer> chain = spec.chain();

        metadata.put("compensable_procs", positions(chain, node -> topology.procedure(chain.get(node))
                .compensateOverridden()));

        // The graph's LEAVES: procedures whose output feeds no other procedure.
        // The operative is DONE when all of them have committed, and the
        // controller derives that from the journal alone — so it needs the set.
        // A linear chain is the degenerate one-leaf case.
        metadata.put("leaf_procs", positions(chain, node -> isLeaf(topology, chain.get(node))));

        return encode(metadata);
    }

    /** A procedure whose output channel feeds no downstream procedure. */
    private static boolean isLeaf(UnboundedOperativeTopology topology, int opIdx) {
        ProcedureDescriptor proc = topology.procedure(opIdx);
        if (proc.outputChannelId().isEmpty()) {
            return true;
        }
        ChannelDescriptor out = topology.channel(proc.outputChannelId().getAsInt());
        return out == null || out.consumers().isEmpty();
    }

    /** Comma-separated tree positions the predicate selects. */
    private static String positions(java.util.List<Integer> chain, java.util.function.IntPredicate select) {
        return IntStream.range(0, chain.size())
                .filter(select)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(","));
    }

    private static byte[] encode(Map<String, String> metadata) {
        if (metadata.isEmpty()) {
            return new byte[0];
        }
        return AddOperativeResponse.newBuilder()
                .putAllMetadata(metadata)
                .build()
                .toByteArray();
    }
}
