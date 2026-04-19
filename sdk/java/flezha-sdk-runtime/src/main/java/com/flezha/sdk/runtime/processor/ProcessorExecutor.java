package com.flezha.sdk.runtime.processor;

import com.flezha.sdk.ProcessorException;
import com.flezha.sdk.ProcessorInput;
import com.flezha.sdk.ProcessorOutput;
import com.flezha.sdk.ProcessorResult;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.stream.SecondaryStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a single {@code (processorId, input)} pair: looks up the
 * processor, runs its pipeline, routes the returned {@link ProcessorResult}.
 *
 * <p>Maps to the Rust SDK's {@code processor::executor::exec_agent}.
 */
public final class ProcessorExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessorExecutor.class);

    private final ComputeEngineContext ctx;

    public ProcessorExecutor(@Nonnull ComputeEngineContext ctx) {
        this.ctx = ctx;
    }

    public void execute(
            @Nonnull String processorId,
            @Nonnull ProcessorInput input,
            long primaryStreamId) {
        ProcessorInstance<?, ?, ?> instance = ctx.processorRegistry().get(processorId);
        if (instance == null) {
            LOG.warn("Processor '{}' not found in compute engine '{}'", processorId, ctx.computeEngineId());
            return;
        }

        ProcessorResult result;
        try {
            result = instance.run(input);
        } catch (ProcessorException e) {
            LOG.warn("Processor '{}' threw: {}", processorId, e.getMessage());
            return;
        } catch (Throwable t) {
            LOG.error("Processor '{}' unexpectedly failed: {}", processorId, t.toString());
            return;
        }

        routeResult(processorId, result, primaryStreamId);
    }

    private void routeResult(String processorId, ProcessorResult result, long primaryStreamId) {
        switch (result) {
            case ProcessorResult.None __ -> {
                // no-op
            }
            case ProcessorResult.Return __ -> {
                // Return is for user-orchestrated flows; the SDK does not pipe it.
            }
            case ProcessorResult.PipeToAgent pipe -> {
                SecondaryStreamWriter writer = resolveWriter(primaryStreamId);
                for (ProcessorResult.OutputWithMetadata out : pipe.outputs()) {
                    writer.writeFrame(processorId, toBytes(out.output()), out.metadata());
                }
            }
            case ProcessorResult.PipeToProcessor pipe -> {
                for (ProcessorResult.Route route : pipe.routes()) {
                    if (ctx.processorRegistry().contains(route.processorId())) {
                        ProcessorInput next = toInput(route.output());
                        execute(route.processorId(), next, primaryStreamId);
                    } else {
                        // Not local — let the agent route it via the secondary stream.
                        SecondaryStreamWriter writer = resolveWriter(primaryStreamId);
                        writer.writeFrame(
                                route.processorId(),
                                toBytes(route.output()),
                                route.metadata());
                    }
                }
            }
        }
    }

    private SecondaryStreamWriter resolveWriter(long primaryStreamId) {
        long secondary = ctx.streamRegistry().secondaryFor(primaryStreamId);
        return new SecondaryStreamWriter(ctx.nativeHandle(), secondary);
    }

    @Nonnull
    private static byte[] toBytes(ProcessorOutput output) {
        return switch (output) {
            case ProcessorOutput.Binary b -> b.data();
            case ProcessorOutput.Text t -> t.value().getBytes(StandardCharsets.UTF_8);
        };
    }

    @Nonnull
    private static ProcessorInput toInput(ProcessorOutput output) {
        return switch (output) {
            case ProcessorOutput.Binary b -> new ProcessorInput.Binary(b.data());
            case ProcessorOutput.Text t -> new ProcessorInput.Text(t.value());
        };
    }

    /** Convert raw primary-stream payload + metadata into a typed {@link ProcessorInput}. */
    @Nonnull
    public static ProcessorInput parseInput(@Nonnull byte[] payload, @Nonnull Map<String, String> metadata) {
        String type = metadata.getOrDefault("input_type", "1");
        return switch (type) {
            case "2" -> new ProcessorInput.Text(new String(payload, StandardCharsets.UTF_8));
            case "1" -> new ProcessorInput.Binary(payload);
            default -> new ProcessorInput.Binary(payload);
        };
    }
}
