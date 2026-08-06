package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.procedure.SpawnException;
import com.flezha.sdk.procedure.SpawnHandle;
import com.flezha.sdk.procedure.SpawnOutcome;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link SpawnRequester} ships a procedure's {@code ctx.spawnBounded} as a
 * framed {@code SpawnBoundedRequest} on the control stream and resolves its
 * handle from the controller's reply. Driven against a capturing
 * {@link SpawnRequester.FrameWriter}, mirroring {@link FactorySyncTest}.
 */
class SpawnRequesterTest {

    private static final long CONTROL_STREAM_ID = 9L;

    @AfterEach
    void clearPendingAcks() {
        AckRegistry.resetForTests();
    }

    @Test
    @DisplayName("the request carries the instance, factory, payload and policy")
    void theRequestCarriesEveryField() throws Exception {
        List<byte[]> captured = new ArrayList<>();
        AtomicReference<Long> streamId = new AtomicReference<>();
        SpawnRequester subject = new SpawnRequester(newCtx(), (id, frame) -> {
            streamId.set(id);
            captured.add(frame);
            return 0;
        });

        subject.request("w1", "worker", "payload".getBytes(), "error");

        assertThat(streamId.get()).isEqualTo(CONTROL_STREAM_ID);
        Frame parsed = parseFirstFrame(captured.get(0));
        assertThat(parsed.payloadId()).isEqualTo("control_command");

        CommandRequest request = CommandRequest.parseFrom(parsed.message());
        assertThat(request.hasSpawnBoundedRequest()).isTrue();
        assertThat(request.getSpawnBoundedRequest().getInstanceId()).isEqualTo("w1");
        assertThat(request.getSpawnBoundedRequest().getFactoryId()).isEqualTo("worker");
        assertThat(request.getSpawnBoundedRequest().getPayload().toStringUtf8()).isEqualTo("payload");
        assertThat(request.getSpawnBoundedRequest().getIfRunning()).isEqualTo("error");
        assertThat(request.getReferenceId()).startsWith("spawn-bounded-w1-");
    }

    @Test
    @DisplayName("the controller's reply resolves the waiter registered for the request")
    void theReplyResolvesTheWaiter() throws Exception {
        List<byte[]> captured = new ArrayList<>();
        SpawnRequester subject = new SpawnRequester(newCtx(), (id, frame) -> {
            captured.add(frame);
            return 0;
        });

        CompletableFuture<String> reply = subject.request("w1", "worker", new byte[] {1}, "deliver");
        String referenceId = CommandRequest.parseFrom(
                        parseFirstFrame(captured.get(0)).message())
                .getReferenceId();

        AckRegistry.complete(referenceId, "delivered");

        assertThat(SpawnHandle.pending("w1", "worker", reply).await()).isEqualTo(SpawnOutcome.DELIVERED);
    }

    @Test
    @DisplayName("a failed write resolves the requester at once instead of leaving it to time out")
    void aFailedWriteResolvesImmediately() {
        SpawnRequester subject = new SpawnRequester(newCtx(), (id, frame) -> 1);

        CompletableFuture<String> reply = subject.request("w1", "worker", new byte[] {1}, "deliver");

        assertThat(reply).isCompletedExceptionally();
        assertThatThrownBy(() -> SpawnHandle.pending("w1", "worker", reply).await())
                .isInstanceOf(SpawnException.Unavailable.class);
    }

    private static RunnerContext newCtx() {
        FlezhaEnv.ResolvedConfig cfg = FlezhaEnv.resolve(Map.of("FLEZHA_WORKER_POOL_SIZE", "1"));
        RunnerContext ctx = new RunnerContext("ns", cfg, 0L);
        ctx.streamRegistry().setControlStreamId(CONTROL_STREAM_ID);
        return ctx;
    }

    private static Frame parseFirstFrame(byte[] bytes) {
        List<Frame> frames = new ArrayList<>();
        new FrameParser().feed(bytes).forEach(frames::add);
        assertThat(frames).hasSize(1);
        return frames.get(0);
    }
}
