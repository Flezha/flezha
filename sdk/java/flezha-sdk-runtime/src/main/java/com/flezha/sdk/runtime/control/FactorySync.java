package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.FactoryDecl;
import com.flezha.sdk.proto.UpdateFactories;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ships the runner's FULL current factory set to the controller as an
 * {@code UpdateFactories} control frame after a post-start define/remove
 * ({@code sdk-runner-entrypoint-redesign}). Fire-and-forget by design: no
 * ack, no waiter — full-set semantics make redelivery idempotent, and the
 * reconnect init frame (built from the same declared set) remains the resync
 * of record, so a lost frame self-heals. Follows {@link GracefulShutdown}'s
 * frame-write pattern (injectable {@link FrameWriter} for tests).
 */
public final class FactorySync {

    private static final Logger LOG = LoggerFactory.getLogger(FactorySync.class);

    /** Wire-level payload_id reused from the SDK-initiated CommandRequest flow. */
    private static final String SYNC_PAYLOAD_ID = "control_command";

    /** Pluggable wire writer so tests can capture frames without a real native context. */
    @FunctionalInterface
    interface FrameWriter {
        /** Returns the same status code semantics as {@link NativeTransport#streamWrite}. */
        int write(long streamId, byte[] frame);
    }

    private final RunnerContext ctx;
    private final FrameWriter writer;

    public FactorySync(@Nonnull RunnerContext ctx) {
        this(ctx, (streamId, frame) -> NativeTransport.streamWrite(ctx.nativeHandle(), streamId, frame));
    }

    /** Test-only constructor — package-private so tests can inject a capturing writer. */
    FactorySync(@Nonnull RunnerContext ctx, @Nonnull FrameWriter writer) {
        this.ctx = ctx;
        this.writer = writer;
    }

    /** Ship {@code factoryIds} as the runner's full current factory set. Best-effort. */
    public void ship(@Nonnull List<String> factoryIds) {
        UpdateFactories.Builder update = UpdateFactories.newBuilder();
        for (String id : factoryIds) {
            update.addFactories(FactoryDecl.newBuilder().setId(id).build());
        }
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("update-factories-" + System.nanoTime())
                .setUpdateFactories(update.build())
                .build();

        byte[] frame = FrameEncoder.build(SYNC_PAYLOAD_ID, request.toByteArray(), Map.of());
        long controlStreamId = ctx.streamRegistry().controlStreamId();
        int status = writer.write(controlStreamId, frame);
        if (status != 0) {
            // A failed write means the control stream is down; the reconnect's
            // init frame carries the same set, so nothing is lost.
            LOG.warn("UpdateFactories write failed (status={}); the reconnect init frame resyncs", status);
        }
    }
}
