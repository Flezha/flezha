package com.flezha.sdk.runtime.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The FlzCore-backed spawn requester, with the blocking ABI faked. */
class CoreSpawnRequesterTest {

    @Test
    @DisplayName("a request runs the spawn ABI off-thread and resolves with its outcome")
    void resolvesWithTheAbiOutcome() throws Exception {
        List<String> calls = new CopyOnWriteArrayList<>();
        CoreSpawnRequester requester = new CoreSpawnRequester(7L, (ctx, instanceId, factoryId, payload, ifRunning) -> {
            calls.add(ctx + "/" + instanceId + "/" + factoryId + "/" + new String(payload, StandardCharsets.UTF_8) + "/"
                    + ifRunning);
            return "spawned";
        });
        CompletableFuture<String> reply =
                requester.request("inst-1", "worker", "go".getBytes(StandardCharsets.UTF_8), "deliver");
        assertThat(reply.get(5, TimeUnit.SECONDS)).isEqualTo("spawned");
        assertThat(calls).containsExactly("7/inst-1/worker/go/deliver");
    }

    @Test
    @DisplayName("an ABI refusal resolves the future exceptionally")
    void refusalResolvesExceptionally() {
        CoreSpawnRequester requester = new CoreSpawnRequester(7L, (ctx, instanceId, factoryId, payload, ifRunning) -> {
            throw new IllegalStateException("already_running");
        });
        CompletableFuture<String> reply = requester.request("inst-1", "worker", new byte[0], "error");
        assertThatThrownBy(() -> reply.get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("already_running");
    }
}
