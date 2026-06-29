package com.flezha.sdk.runtime.bounded;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.BoundedCompleted;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.bounded.BoundedOperative;
import com.flezha.sdk.bounded.BoundedOperativeCtx;
import com.flezha.sdk.bounded.BoundedOperativeException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run-once driver for a bounded-kind operative. Scheduled once on the worker pool at
 * spawn time (from {@code AddOperativeHandler}); it instantiates the bounded,
 * invokes {@link BoundedOperative#run} exactly once, then emits a {@code BoundedCompleted}
 * control message so the controller tears the operative down. Output emitted via
 * {@link BoundedOperativeCtx#emit} mirrors the Rust SDK's {@code write_bounded_output}: a
 * frame whose payload id is the instance id and whose metadata is empty,
 * written on a secondary stream.
 */
public final class BoundedOperativeRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BoundedOperativeRunner.class);

    /** Wire-level payload id for SDK→controller control commands (same as the operative path). */
    private static final String CONTROL_PAYLOAD_ID = "control_command";

    /**
     * Indirection over {@link NativeTransport#streamWrite(long, long, byte[])}
     * so tests can capture bounded output / completion frames without a real
     * native context. Mirrors {@code OperationDispatcher.WIRE_WRITER}.
     */
    public interface WireWriter {
        int write(long handle, long streamId, byte[] frame);
    }

    private static volatile WireWriter wireWriter = NativeTransport::streamWrite;

    /** Test seam: install a capturing writer. */
    public static void setWireWriter(@Nonnull WireWriter writer) {
        wireWriter = writer;
    }

    /** Restore the default {@link NativeTransport#streamWrite} writer. */
    public static void resetWireWriter() {
        wireWriter = NativeTransport::streamWrite;
    }

    private BoundedOperativeRunner() {}

    /**
     * Schedule the bounded's {@code run} once on the worker pool. Returns
     * immediately; completion is signalled asynchronously via
     * {@code BoundedCompleted}.
     */
    public static void start(
            @Nonnull ExecutorContext ctx,
            @Nonnull String instanceId,
            @Nonnull Class<? extends BoundedOperative> boundedClass,
            @Nonnull String args) {
        ctx.workerPool().execute(() -> runOnce(ctx, instanceId, boundedClass, args));
    }

    private static void runOnce(ExecutorContext ctx, String instanceId, Class<? extends BoundedOperative> boundedClass, String args) {
        String status = "ok";
        String error = "";
        try {
            BoundedOperative bounded = boundedClass.getDeclaredConstructor().newInstance();
            bounded.run(new BoundedOperativeCtxImpl(ctx, instanceId), args);
        } catch (BoundedOperativeException e) {
            status = "error";
            error = e.getMessage() == null ? e.toString() : e.getMessage();
        } catch (Throwable t) {
            // Any unchecked failure (including a reflective ctor failure) is a
            // terminal bounded error — never let it escape the worker thread.
            status = "error";
            error = t.getMessage() == null ? t.toString() : t.getMessage();
            LOG.warn("BoundedOperative {} run failed: {}", instanceId, t.toString());
        }
        emitCompletion(ctx, instanceId, status, error);
    }

    private static void emitCompletion(ExecutorContext ctx, String instanceId, String status, String error) {
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("bounded-completed-" + instanceId + "-" + System.nanoTime())
                .setBoundedCompleted(BoundedCompleted.newBuilder()
                        .setOperativeId(instanceId)
                        .setStatus(status)
                        .setError(error)
                        .build())
                .build();
        byte[] frame = FrameEncoder.build(CONTROL_PAYLOAD_ID, request.toByteArray(), Map.of());
        long controlStreamId = ctx.streamRegistry().controlStreamId();
        int writeStatus = wireWriter.write(ctx.nativeHandle(), controlStreamId, frame);
        if (writeStatus != 0) {
            LOG.warn("BoundedCompleted write failed for {} (status={})", instanceId, writeStatus);
        }
    }

    /** {@link BoundedOperativeCtx} bound to one bounded instance; emits instance-id-keyed, empty-metadata frames. */
    private static final class BoundedOperativeCtxImpl implements BoundedOperativeCtx {
        private final ExecutorContext ctx;
        private final String instanceId;

        BoundedOperativeCtxImpl(ExecutorContext ctx, String instanceId) {
            this.ctx = ctx;
            this.instanceId = instanceId;
        }

        @Override
        public void emit(@Nonnull byte[] value) throws BoundedOperativeException {
            long secondaryStreamId = ctx.streamRegistry().anySecondary();
            if (secondaryStreamId == 0L) {
                LOG.warn("bounded {} output dropped: no secondary stream available", instanceId);
                return;
            }
            byte[] frame = FrameEncoder.build(instanceId, value, Map.of());
            int writeStatus = wireWriter.write(ctx.nativeHandle(), secondaryStreamId, frame);
            if (writeStatus != 0) {
                throw new BoundedOperativeException("bounded " + instanceId + " output write failed (status " + writeStatus + ")");
            }
        }
    }
}
