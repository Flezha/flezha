package com.flezha.sdk.runtime.stream;

import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.processor.ProcessorExecutor;
import com.flezha.sdk.runtime.worker.ProcessorDispatcher;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-primary-stream frame parser + dispatcher. Driven by the JNI
 * {@code onData} callback. Each incoming chunk feeds into a local
 * {@link FrameParser}, and every decoded {@link Frame} is dispatched to the
 * target processor via {@link ProcessorDispatcher}.
 */
public final class PrimaryStreamReader {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryStreamReader.class);

    private final ComputeEngineContext ctx;
    private final long streamId;
    private final FrameParser parser = new FrameParser();

    public PrimaryStreamReader(@Nonnull ComputeEngineContext ctx, long streamId) {
        this.ctx = ctx;
        this.streamId = streamId;
    }

    public void onData(@Nonnull byte[] data) {
        try {
            for (Frame frame : parser.feed(data)) {
                dispatch(frame);
            }
        } catch (Exception e) {
            LOG.error("Primary stream {} frame decode failed: {}", streamId, e.toString());
        }
    }

    private void dispatch(Frame frame) {
        ProcessorDispatcher dispatcher = ctx.processorDispatcher();
        if (dispatcher == null) {
            // No dispatcher configured yet — fall back to inline execution (no concurrency).
            new ProcessorExecutor(ctx).execute(
                    frame.payloadId(),
                    ProcessorExecutor.parseInput(frame.message(), frame.metadata()),
                    streamId);
            return;
        }
        dispatcher.enqueue(frame.payloadId(),
                ProcessorExecutor.parseInput(frame.message(), frame.metadata()),
                streamId);
    }
}
