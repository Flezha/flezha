package com.flezha.sdk.runtime.control;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.task.TaskRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JSON payload sent with the {@code connect_control_channel} frame. Matches
 * the Rust SDK's {@code ControlInitMessage} field names exactly —
 * {@code namespace}, {@code factory_ids}, {@code factories},
 * {@code previous_executor_id}.
 *
 * <p>{@code factories} carries a per-Relay description record. The
 * Relay interface has no description hook today, so the per-id
 * description is always empty.
 *
 * <p>{@code previous_executor_id} is the node-locally persisted id from a prior
 * process on this node, announced on the INITIAL connect only (omitted —
 * {@code null}, dropped by {@code NON_NULL} — on reconnect) so the controller
 * can evict the stale binding before re-binding. Mirrors the Rust SDK.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HandshakeMessage(
        String namespace, List<String> factory_ids, List<FactoryInfo> factories, String previous_executor_id) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FactoryInfo(String id, String description) {}

    public static HandshakeMessage from(String namespace) {
        return from(namespace, null);
    }

    public static HandshakeMessage from(String namespace, String previousExecutorId) {
        // Relay factories first, then task factories — both kinds are advertised
        // in one combined list so the controller validates any AddRelay kind.
        List<String> ids = new ArrayList<>(RelayRegistry.factoryIds());
        ids.addAll(TaskRegistry.factoryIds());
        List<FactoryInfo> infos =
                ids.stream().map(id -> new FactoryInfo(id, "")).collect(Collectors.toList());
        return new HandshakeMessage(namespace, ids, infos, previousExecutorId);
    }
}
