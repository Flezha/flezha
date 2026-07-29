package com.flezha.sdk.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Operation-requested spawn, api half ({@code add-operation-requested-spawn}):
 * the result mapping, the isolation from {@code run}, and the replay contract.
 * Mirrors the Rust {@code operation::tests::spawn} suite scenario for scenario.
 */
class SpawnHandleTest {

    @AfterEach
    void dropRequester() {
        SpawnRequests.uninstall();
    }

    private static SpawnHandle answering(String outcome) {
        return SpawnHandle.pending("w1", "worker", CompletableFuture.completedFuture(outcome));
    }

    @Test
    @DisplayName("a spawn and a delivery are distinguishable outcomes")
    void spawnAndDeliveryAreDistinguishable() throws Exception {
        assertThat(answering("spawned").await()).isEqualTo(SpawnOutcome.SPAWNED);
        assertThat(answering("delivered").await()).isEqualTo(SpawnOutcome.DELIVERED);
    }

    @Test
    @DisplayName("the error policy and an unknown factory come back as errors")
    void policyAndUnknownFactoryAreErrors() {
        assertThatThrownBy(() -> answering("already_running").await())
                .isInstanceOf(SpawnException.AlreadyRunning.class)
                .hasMessageContaining("w1");
        assertThatThrownBy(() -> answering("factory_unknown").await())
                .isInstanceOf(SpawnException.FactoryUnknown.class)
                .hasMessageContaining("worker");
    }

    @Test
    @DisplayName("a refusal carries the controller's reason")
    void refusalCarriesItsReason() {
        SpawnHandle handle = SpawnHandle.pending(
                "w1", "worker", CompletableFuture.failedFuture(new SpawnException.Refused("quota_exceeded")));

        assertThatThrownBy(handle::await)
                .isInstanceOf(SpawnException.Refused.class)
                .hasMessageContaining("quota_exceeded");
    }

    @Test
    @DisplayName("tryResult leaves a pending handle pending")
    void tryResultLeavesAPendingHandlePending() throws Exception {
        CompletableFuture<String> reply = new CompletableFuture<>();
        SpawnHandle handle = SpawnHandle.pending("w1", "worker", reply);

        assertThat(handle.tryResult()).isEmpty();

        reply.complete("spawned");
        assertThat(handle.tryResult()).contains(SpawnOutcome.SPAWNED);
    }

    @Test
    @DisplayName("without a control plane the request resolves instead of blocking the operation")
    void withoutAControlPlaneTheRequestResolves() {
        assertThatThrownBy(() -> SpawnRequests.request("w1", "worker", new byte[] {1}, IfRunning.DELIVER, false)
                        .await())
                .isInstanceOf(SpawnException.Unavailable.class);
    }

    @Test
    @DisplayName("an ignored handle is not an error for the operation")
    void anIgnoredHandleIsNotAnError() {
        SpawnRequests.install((instanceId, factoryId, payload, ifRunning) ->
                CompletableFuture.failedFuture(new SpawnException.Refused("nope")));

        // Fire-and-forget: never inspecting the handle must not throw. The
        // failure it carries is reported to the log, not to the caller.
        SpawnRequests.request("w1", "worker", new byte[] {1}, IfRunning.DELIVER, false);
    }

    @Test
    @DisplayName("a replayed invocation issues no request")
    void aReplayedInvocationIssuesNoRequest() throws Exception {
        SpawnRequests.install((instanceId, factoryId, payload, ifRunning) -> {
            throw new AssertionError("a replay must not reach the control plane");
        });

        assertThat(SpawnRequests.request("w1", "worker", new byte[] {1}, IfRunning.DELIVER, true)
                        .await())
                .isEqualTo(SpawnOutcome.SKIPPED_ON_REPLAY);
    }

    @Test
    @DisplayName("a live invocation does issue one, carrying the policy the controller reads")
    void aLiveInvocationIssuesOne() throws Exception {
        StringBuilder seen = new StringBuilder();
        SpawnRequests.install((instanceId, factoryId, payload, ifRunning) -> {
            seen.append(instanceId).append('/').append(factoryId).append('/').append(ifRunning);
            return CompletableFuture.completedFuture("spawned");
        });

        assertThat(SpawnRequests.request("w1", "worker", new byte[] {1}, IfRunning.ERROR, false)
                        .await())
                .isEqualTo(SpawnOutcome.SPAWNED);
        assertThat(seen.toString()).isEqualTo("w1/worker/error");
    }

    @Test
    @DisplayName("the policy is carried on the wire as the controller reads it")
    void thePolicyIsCarriedOnTheWire() {
        assertThat(IfRunning.DELIVER.wire()).isEqualTo("deliver");
        assertThat(IfRunning.ERROR.wire()).isEqualTo("error");
    }

    @Test
    @DisplayName("the default OperationCtx implementation routes through the installed requester")
    void theCtxDefaultRoutesThroughTheRequester() throws Exception {
        SpawnRequests.install(
                (instanceId, factoryId, payload, ifRunning) -> CompletableFuture.completedFuture("delivered"));

        OperationCtx live = new StubCtx(false);
        assertThat(live.spawnBounded("w1", "worker", new byte[] {1}, IfRunning.DELIVER)
                        .await())
                .isEqualTo(SpawnOutcome.DELIVERED);

        OperationCtx replaying = new StubCtx(true);
        assertThat(replaying
                        .spawnBounded("w1", "worker", new byte[] {1}, IfRunning.DELIVER)
                        .await())
                .isEqualTo(SpawnOutcome.SKIPPED_ON_REPLAY);
    }

    /** Only the two members {@code spawnBounded}'s default implementation reads. */
    private record StubCtx(boolean replay) implements OperationCtx {
        @Override
        public boolean replaying() {
            return replay;
        }

        @Override
        public long now() {
            return 0;
        }

        @Override
        public StableId stableId() {
            return new StableId(0, 0);
        }

        @Override
        public void checkpoint() {}

        @Override
        public <C> void emitKeyed(String key, C value) {}

        @Override
        public <C> void emitKeyedWithMeta(String key, C value, java.util.Map<String, String> meta) {}

        @Override
        public <C> void emit(C value) {}

        @Override
        public <C> void emit(String channel, C value) {}

        @Override
        public <C> void emitWithMeta(C value, java.util.Map<String, String> meta) {}

        @Override
        public <C> void emitWithMeta(String channel, C value, java.util.Map<String, String> meta) {}
    }
}
