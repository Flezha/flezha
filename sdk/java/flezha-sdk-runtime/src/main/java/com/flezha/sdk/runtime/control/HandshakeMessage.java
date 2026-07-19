package com.flezha.sdk.runtime.control;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.flezha.sdk.runtime.RunnerIdStore;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JSON payload sent with the {@code connect_control_channel} frame. Matches
 * the Rust SDK's {@code ControlInitMessage} field names exactly —
 * {@code namespace}, {@code factory_ids}, {@code factories}, {@code stable_id}.
 *
 * <p>{@code factories} carries a per-UnboundedOperative description record. The
 * UnboundedOperative interface has no description hook today, so the per-id
 * description is always empty.
 *
 * <p>{@code stable_id} is the runner's self-owned, durable stable identity
 * (see {@link RunnerIdStore}). It is present on EVERY connect — initial and
 * reconnect alike — and trusted by the controller as the operative-binding key.
 * Mirrors the Rust SDK.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HandshakeMessage(
        String namespace, List<String> factory_ids, List<FactoryInfo> factories, String stable_id) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record FactoryInfo(String id, String description) {}

    /**
     * Build the init payload from the runner's per-runner declared factory
     * set (unbounded first, then bounded — one combined list so the
     * controller validates any AddOperative kind). Read at send time, so
     * every reconnect naturally resyncs post-start defines/removes — the
     * init frame is the resync of record.
     */
    public static HandshakeMessage from(String namespace, List<String> ids) {
        List<FactoryInfo> infos =
                ids.stream().map(id -> new FactoryInfo(id, "")).collect(Collectors.toList());
        return new HandshakeMessage(namespace, ids, infos, RunnerIdStore.stableId(namespace, ids));
    }
}
