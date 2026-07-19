package com.flezha.sdk.runtime.control;

import static org.assertj.core.api.Assertions.assertThat;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.FactoryDecl;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link FactorySync} ships the runner's FULL current factory set as a framed
 * {@code UpdateFactories} control request — fire-and-forget, on the current
 * control stream. Driven against a capturing
 * {@link FactorySync.FrameWriter}, mirroring {@code GracefulShutdownTest}.
 */
class FactorySyncTest {

    private static final long CONTROL_STREAM_ID = 9L;

    @Test
    @DisplayName("ship() emits one framed UpdateFactories carrying the full set in order")
    void shipsTheFullFactorySet() throws Exception {
        RunnerContext ctx = newCtx();

        List<byte[]> captured = new ArrayList<>();
        AtomicReference<Long> capturedStreamId = new AtomicReference<>();
        FactorySync subject = new FactorySync(ctx, (streamId, frame) -> {
            capturedStreamId.set(streamId);
            captured.add(frame);
            return 0;
        });

        subject.ship(List.of("alpha", "beta"));

        assertThat(captured).hasSize(1);
        assertThat(capturedStreamId.get()).isEqualTo(CONTROL_STREAM_ID);

        Frame parsed = parseFirstFrame(captured.get(0));
        assertThat(parsed.payloadId()).isEqualTo("control_command");

        CommandRequest request = CommandRequest.parseFrom(parsed.message());
        assertThat(request.hasUpdateFactories()).isTrue();
        assertThat(request.getUpdateFactories().getFactoriesList().stream()
                        .map(FactoryDecl::getId)
                        .toList())
                .containsExactly("alpha", "beta");
    }

    @Test
    @DisplayName("an empty set ships as an empty UpdateFactories (zero placeable factories is legal)")
    void shipsAnEmptySet() throws Exception {
        RunnerContext ctx = newCtx();

        List<byte[]> captured = new ArrayList<>();
        FactorySync subject = new FactorySync(ctx, (streamId, frame) -> {
            captured.add(frame);
            return 0;
        });

        subject.ship(List.of());

        assertThat(captured).hasSize(1);
        CommandRequest request =
                CommandRequest.parseFrom(parseFirstFrame(captured.get(0)).message());
        assertThat(request.hasUpdateFactories()).isTrue();
        assertThat(request.getUpdateFactories().getFactoriesCount()).isZero();
    }

    @Test
    @DisplayName("a failed write is swallowed — the reconnect init frame is the resync of record")
    void aFailedWriteDoesNotThrow() {
        FactorySync subject = new FactorySync(newCtx(), (streamId, frame) -> 1);
        subject.ship(List.of("alpha"));
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
