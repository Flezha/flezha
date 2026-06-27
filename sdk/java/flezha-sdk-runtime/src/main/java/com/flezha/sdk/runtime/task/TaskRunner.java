package com.flezha.sdk.runtime.task;

import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.TaskCompleted;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.FrameEncoder;
import com.flezha.sdk.task.Task;
import com.flezha.sdk.task.TaskCtx;
import com.flezha.sdk.task.TaskException;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run-once driver for a task-kind unit. Scheduled once on the worker pool at
 * spawn time (from {@code AddRelayHandler}); it instantiates the task,
 * invokes {@link Task#run} exactly once, then emits a {@code TaskCompleted}
 * control message so the controller tears the unit down. Output emitted via
 * {@link TaskCtx#emit} mirrors the Rust SDK's {@code write_task_output}: a
 * frame whose payload id is the instance id and whose metadata is empty,
 * written on a secondary stream.
 */
public final class TaskRunner {

    private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

    /** Wire-level payload id for SDK→controller control commands (same as the relay path). */
    private static final String CONTROL_PAYLOAD_ID = "control_command";

    /**
     * Indirection over {@link NativeTransport#streamWrite(long, long, byte[])}
     * so tests can capture task output / completion frames without a real
     * native context. Mirrors {@code ShardDispatcher.WIRE_WRITER}.
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

    private TaskRunner() {}

    /**
     * Schedule the task's {@code run} once on the worker pool. Returns
     * immediately; completion is signalled asynchronously via
     * {@code TaskCompleted}.
     */
    public static void start(
            @Nonnull ExecutorContext ctx,
            @Nonnull String instanceId,
            @Nonnull Class<? extends Task> taskClass,
            @Nonnull String args) {
        ctx.workerPool().execute(() -> runOnce(ctx, instanceId, taskClass, args));
    }

    private static void runOnce(ExecutorContext ctx, String instanceId, Class<? extends Task> taskClass, String args) {
        String status = "ok";
        String error = "";
        try {
            Task task = taskClass.getDeclaredConstructor().newInstance();
            task.run(new TaskCtxImpl(ctx, instanceId), args);
        } catch (TaskException e) {
            status = "error";
            error = e.getMessage() == null ? e.toString() : e.getMessage();
        } catch (Throwable t) {
            // Any unchecked failure (including a reflective ctor failure) is a
            // terminal task error — never let it escape the worker thread.
            status = "error";
            error = t.getMessage() == null ? t.toString() : t.getMessage();
            LOG.warn("Task {} run failed: {}", instanceId, t.toString());
        }
        emitCompletion(ctx, instanceId, status, error);
    }

    private static void emitCompletion(ExecutorContext ctx, String instanceId, String status, String error) {
        CommandRequest request = CommandRequest.newBuilder()
                .setReferenceId("task-completed-" + instanceId + "-" + System.nanoTime())
                .setTaskCompleted(TaskCompleted.newBuilder()
                        .setRelayId(instanceId)
                        .setStatus(status)
                        .setError(error)
                        .build())
                .build();
        byte[] frame = FrameEncoder.build(CONTROL_PAYLOAD_ID, request.toByteArray(), Map.of());
        long controlStreamId = ctx.streamRegistry().controlStreamId();
        int writeStatus = wireWriter.write(ctx.nativeHandle(), controlStreamId, frame);
        if (writeStatus != 0) {
            LOG.warn("TaskCompleted write failed for {} (status={})", instanceId, writeStatus);
        }
    }

    /** {@link TaskCtx} bound to one task instance; emits instance-id-keyed, empty-metadata frames. */
    private static final class TaskCtxImpl implements TaskCtx {
        private final ExecutorContext ctx;
        private final String instanceId;

        TaskCtxImpl(ExecutorContext ctx, String instanceId) {
            this.ctx = ctx;
            this.instanceId = instanceId;
        }

        @Override
        public void emit(@Nonnull byte[] value) throws TaskException {
            long secondaryStreamId = ctx.streamRegistry().anySecondary();
            if (secondaryStreamId == 0L) {
                LOG.warn("task {} output dropped: no secondary stream available", instanceId);
                return;
            }
            byte[] frame = FrameEncoder.build(instanceId, value, Map.of());
            int writeStatus = wireWriter.write(ctx.nativeHandle(), secondaryStreamId, frame);
            if (writeStatus != 0) {
                throw new TaskException("task " + instanceId + " output write failed (status " + writeStatus + ")");
            }
        }
    }
}
