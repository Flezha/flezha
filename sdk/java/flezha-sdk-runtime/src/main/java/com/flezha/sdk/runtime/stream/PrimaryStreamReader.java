package com.flezha.sdk.runtime.stream;

import com.flezha.sdk.channel.ChannelKind;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.ChannelDescriptor;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeTopology;
import com.flezha.sdk.runtime.worker.OperationDispatcher;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-primary-stream frame parser + dispatcher. Driven by the JNI
 * {@code onData} callback. Each incoming chunk feeds into a local
 * {@link FrameParser}; for each decoded {@link Frame} the reader looks up
 * the {@link UnboundedOperativeInstance} by {@code payloadId} (= operative instance id) and
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

    /** Offset of this input in the controller's durable input log (tier only). */
    private static final String OFFSET_META_KEY = "flz.offset";

    /** Marks an input as a member of a correction replay slice. */
    private static final String REPLAY_META_KEY = "flz.replay";

    /** Durable inbox offset of a looping bounded operative's message. */
    private static final String INBOX_OFFSET_META_KEY = "flz.inbox_offset";

    /** Payload-less broadcast closing an open replay slice on every Segment. */
    private static final String REPLAY_END_META_KEY = "flz.replay_end";

    private static OptionalLong parseOffset(String raw) {
        if (raw == null || raw.isEmpty()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(raw));
        } catch (NumberFormatException e) {
            LOG.warn("dispatch: unparseable {} `{}`; treating the input as unjournaled", OFFSET_META_KEY, raw);
            return OptionalLong.empty();
        }
    }

    private final RunnerContext ctx;
    private final long streamId;
    private final FrameParser parser = new FrameParser();

    public PrimaryStreamReader(@Nonnull RunnerContext ctx, long streamId) {
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
        // Resolve the operative kind once per frame. A bounded's data frame is
        // post-start input: route it to the running driver's inbox.
        UnboundedOperativeInstance instance;
        switch (operative) {
            case UnboundedOperativeInstance ri -> instance = ri;
            case BoundedOperativeInstance bounded -> {
                // The controller stamps the durable inbox offset on a looping
                // bounded's message; the driver journals it at the iteration
                // boundary, so a resumed loop does not re-consume it.
                parseOffset(frame.metadata().get(INBOX_OFFSET_META_KEY)).ifPresent(bounded::noteInboxOffset);
                if (!bounded.offerInbox(frame.message())) {
                    LOG.warn(
                            "dispatch: inbox frame for bounded `{}` dropped (inbox full / not running)",
                            operativeInstanceId);
                }
                return;
            }
        }
        UnboundedOperativeTopology topology = instance.topology();

        // `flz.replay_end` is a payload-less BROADCAST, intercepted before any
        // channel lookup. It cannot ride on the last input of the slice: an input
        // routes to exactly ONE Segment by key, so a marker carried that way would
        // close one Segment's slice and strand the slice every other Segment
        // buffered.
        if (frame.metadata().containsKey(REPLAY_END_META_KEY)) {
            for (int operationId = 0; operationId < topology.operations().size(); operationId++) {
                OperationDispatcher dispatcher = instance.dispatcher(operationId);
                if (dispatcher != null) {
                    dispatcher.endReplaySliceAllSegments();
                }
            }
            return;
        }

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
        // The controller stamps `flz.offset` on an input it journaled to the
        // durable input log. The effectively-once tier acknowledges it in the
        // same WAL record as the invocation's state and output, which is what
        // makes the offset advance and the effect atomic.
        OptionalLong offset = parseOffset(frame.metadata().get(OFFSET_META_KEY));
        boolean replay = frame.metadata().containsKey(REPLAY_META_KEY);
        for (int consumerOperationId : channel.consumers()) {
            OperationDispatcher dispatcher = instance.dispatcher(consumerOperationId);
            if (dispatcher != null) {
                dispatcher.fanoutSegments(input, streamId, offset, replay);
            }
        }
    }

    private static ChannelDescriptor resolveEntryChannel(UnboundedOperativeTopology topology, String channelName) {
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
