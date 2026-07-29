package com.flezha.sdk.runtime.control;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.operation.SpawnException;
import com.flezha.sdk.operation.SpawnRequests;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.SpawnBoundedRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/**
 * Ships an operation's {@code ctx.spawnBounded} as a {@code SpawnBoundedRequest}
 * control frame and hands back the pending reply
 * ({@code add-operation-requested-spawn}).
 *
 * <p>Registers the waiter with {@link AckRegistry} <em>before</em> writing, the
 * ordering that makes the rendezvous race-free. A failed write resolves the
 * waiter at once rather than leaving the requester to time out. Follows
 * {@link FactorySync}'s frame-write pattern (injectable {@link FrameWriter} for
 * tests).
 */
public final class SpawnRequester implements SpawnRequests.Requester {

    /** Wire-level payload_id reused from the SDK-initiated CommandRequest flow. */
    private static final String SPAWN_PAYLOAD_ID = "control_command";

    /** Pluggable wire writer so tests can capture frames without a real native context. */
    @FunctionalInterface
    interface FrameWriter {
        /** Returns the same status code semantics as {@link NativeTransport#streamWrite}. */
        int write(long streamId, byte[] frame);
    }

    private final RunnerContext ctx;
    private final FrameWriter writer;

    public SpawnRequester(@Nonnull RunnerContext ctx) {
        this(ctx, (streamId, frame) -> NativeTransport.streamWrite(ctx.nativeHandle(), streamId, frame));
    }

    /** Test-only constructor — package-private so tests can inject a capturing writer. */
    SpawnRequester(@Nonnull RunnerContext ctx, @Nonnull FrameWriter writer) {
        this.ctx = ctx;
        this.writer = writer;
    }

    @Override
    @Nonnull
    public CompletableFuture<String> request(
            @Nonnull String instanceId, @Nonnull String factoryId, @Nonnull byte[] payload, @Nonnull String ifRunning) {
        String referenceId = "spawn-bounded-" + instanceId + "-" + System.nanoTime();
        CompletableFuture<String> reply = AckRegistry.register(referenceId);

        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId(referenceId)
                .setSpawnBoundedRequest(SpawnBoundedRequest.newBuilder()
                        .setInstanceId(instanceId)
                        .setFactoryId(factoryId)
                        .setPayload(ByteString.copyFrom(payload))
                        .setIfRunning(ifRunning)
                        .build())
                .build();

        byte[] frame = FrameEncoder.build(SPAWN_PAYLOAD_ID, request.toByteArray(), Map.of());
        int status = writer.write(ctx.streamRegistry().controlStreamId(), frame);
        if (status != 0) {
            AckRegistry.fail(referenceId, new SpawnException.Unavailable());
        }
        return reply;
    }
}
