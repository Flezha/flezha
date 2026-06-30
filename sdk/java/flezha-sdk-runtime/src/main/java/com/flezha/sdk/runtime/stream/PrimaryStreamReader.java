package com.flezha.sdk.runtime.stream;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.ContinuousOperativeInstance;
import com.flezha.sdk.runtime.operative.ContinuousOperativeTopology;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.worker.OperationDispatcher;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-primary-stream frame parser + dispatcher. Driven by the JNI
 * {@code onData} callback. Each incoming chunk feeds into a local
 * {@link FrameParser}; for each decoded {@link Frame} the reader looks up
 * the {@link ContinuousOperativeInstance} by {@code payloadId} (= operative instance id) and
 * routes the input to the consumer Operations declared on the entry channel
 * named in {@code metadata["channel"]}.
 *
 * <p>If the {@code channel} metadata key is missing, the reader falls back
 * to the operative's first declared input channel (the default per
 * {@code docs/sdk_protocol_spec.md} §3.4).
 */
public final class PrimaryStreamReader {

    private static final Logger LOG = LoggerFactory.getLogger(PrimaryStreamReader.class);
    private static final String CHANNEL_META_KEY = "channel";

    private final ExecutorContext ctx;
    private final long streamId;
    private final FrameParser parser = new FrameParser();

    public PrimaryStreamReader(@Nonnull ExecutorContext ctx, long streamId) {
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
        String operativeInstanceId = frame.payloadId();
        Operative operative = ctx.operativeInstanceRegistry().get(operativeInstanceId);
        if (operative == null) {
            LOG.warn("dispatch: operative instance `{}` not found on stream {}", operativeInstanceId, streamId);
            return;
        }
        // Resolve the operative kind once per frame. A bounded has no input channel,
        // so a data frame addressed to it is a logged no-op.
        ContinuousOperativeInstance instance;
        switch (operative) {
            case ContinuousOperativeInstance ri -> instance = ri;
            case BoundedOperativeInstance ignored -> {
                LOG.warn(
                        "dispatch: data frame for bounded `{}` dropped (boundeds have no input channel)",
                        operativeInstanceId);
                return;
            }
        }
        ContinuousOperativeTopology topology = instance.topology();

        String channelName = frame.metadata().get(CHANNEL_META_KEY);
        ChannelDescriptor channel = resolveEntryChannel(topology, channelName);
        if (channel == null) {
            LOG.warn(
                    "dispatch: no entry channel resolvable for operative `{}` (channel meta = `{}`)",
                    operativeInstanceId,
                    channelName);
            return;
        }

        Object input = wrapInput(channel, frame.message());
        for (int consumerOperationId : channel.consumers()) {
            OperationDispatcher dispatcher = instance.dispatcher(consumerOperationId);
            if (dispatcher != null) {
                dispatcher.fanoutSegments(input, streamId);
            }
        }
    }

    private static ChannelDescriptor resolveEntryChannel(ContinuousOperativeTopology topology, String channelName) {
        if (channelName != null) {
            Integer id = topology.lookupChannelId(channelName);
            if (id != null) {
                ChannelDescriptor d = topology.channel(id);
                if (d != null && d.kind() == ChannelKind.ENTRY) {
                    return d;
                }
            }
            return null;
        }
        // Fall back to the first declared entry channel (per the spec's
        // missing-channel-metadata default).
        for (ChannelDescriptor d : topology.channels()) {
            if (d.kind() == ChannelKind.ENTRY) {
                return d;
            }
        }
        return null;
    }

    private static Object wrapInput(ChannelDescriptor channel, byte[] payload) {
        if (channel.payloadType() == String.class) {
            return new String(payload, StandardCharsets.UTF_8);
        }
        return payload;
    }
}
