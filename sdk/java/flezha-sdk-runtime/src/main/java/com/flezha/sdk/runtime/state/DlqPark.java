package com.flezha.sdk.runtime.state;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dead-letter park: retry-exhausted payloads ship to the controller's
 * durable DLQ store and the segment advances only once the park is durable
 * ({@code platform-dead-letter-queue}). Mirrors the Rust SDK's
 * {@code segment::park}: a {@code park_payload} data-stream frame on the
 * reliable sink plus a monitor wait for the controller's
 * {@code ParkPayloadAck} control command, re-sending until acked — delay,
 * never loss.
 */
public final class DlqPark {

    private static final Logger LOG = LoggerFactory.getLogger(DlqPark.class);

    /** Data-stream payload-id for a dead-letter park (SDK → controller). */
    public static final String PAYLOAD_ID_PARK = "park_payload";

    /** Process-global count of retry attempts after a failed invocation. */
    public static final AtomicLong RETRIES_TOTAL = new AtomicLong();

    /** Process-global count of payloads parked to the dead-letter queue. */
    public static final AtomicLong PARKED_TOTAL = new AtomicLong();

    /** Monotonic nonce making park ids unique within a process. */
    private static final AtomicLong PARK_SEQ = new AtomicLong();

    private static final Object ACK_LOCK = new Object();
    private static final Set<String> acked = new HashSet<>();

    private static final long ACK_RESEND_INTERVAL_MS = 2_000L;
    private static final long WARN_EVERY_RESENDS = 5L;

    private DlqPark() {}

    /**
     * The park's dead-letter record — the same shape a tier-on park embeds
     * in its effect record (Rust {@code DlqEntryBody}). {@code payload} is
     * empty when the input type is not byte-representable (inter-operation
     * payloads other than {@code byte[]}/{@code String}); such entries are
     * inspectable but not replayable.
     */
    public record DlqEntry(String channel, String error, int attempts, Optional<byte[]> payload) {}

    /**
     * Wire body of a {@code park_payload} frame. Postcard-encoded by
     * {@link #encodeParkBody} in this exact field order — the cross-language
     * contract with the Rust SDK's {@code ParkBody} and the controller's
     * decode.
     */
    public record ParkBody(String parkId, OptionalLong originOffset, DlqEntry entry) {}

    /** Mint a process-unique park id. */
    @Nonnull
    public static String mintParkId(@Nonnull String segmentLabel) {
        return segmentLabel + "#" + PARK_SEQ.getAndIncrement();
    }

    /**
     * Record a controller ack for {@code parkId} and wake the parked
     * dispatcher. Called from the {@code ParkPayloadAck} control handler;
     * unknown ids are retained (the ack can race ahead of the waiter).
     */
    public static void completeAck(@Nonnull String parkId) {
        synchronized (ACK_LOCK) {
            acked.add(parkId);
            ACK_LOCK.notifyAll();
        }
    }

    /**
     * Recover the wire bytes of an input for the DLQ entry. Entry-channel
     * payloads are {@code byte[]}/{@code String} and round-trip exactly;
     * other input types have no serialization contract and park without
     * bytes.
     */
    @Nonnull
    public static Optional<byte[]> extractPayloadBytes(@Nonnull Object input) {
        if (input instanceof byte[] bytes) {
            return Optional.of(bytes.clone());
        }
        if (input instanceof String s) {
            return Optional.of(s.getBytes(StandardCharsets.UTF_8));
        }
        return Optional.empty();
    }

    /**
     * Postcard encoding of a {@link ParkBody}: LEB128 varint lengths/ints,
     * UTF-8 strings, {@code Option} as {@code 0x00}/{@code 0x01}+value.
     * Byte-identical to the Rust SDK's {@code postcard::to_allocvec(ParkBody)}.
     */
    @Nonnull
    public static byte[] encodeParkBody(@Nonnull ParkBody body) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeString(out, body.parkId());
        if (body.originOffset().isPresent()) {
            out.write(1);
            writeVarint(out, body.originOffset().getAsLong());
        } else {
            out.write(0);
        }
        DlqEntry entry = body.entry();
        writeString(out, entry.channel());
        writeString(out, entry.error());
        writeVarint(out, Integer.toUnsignedLong(entry.attempts()));
        if (entry.payload().isPresent()) {
            out.write(1);
            writeBytes(out, entry.payload().get());
        } else {
            out.write(0);
        }
        return out.toByteArray();
    }

    /**
     * Ship a park frame and block until the controller acks it as durable.
     * Re-sends on the resend cadence forever — the segment must not advance
     * past an unparked poison payload. Transport-less runs (no sink ever
     * installed) cannot park; they log loudly and drop, preserving the
     * pre-DLQ standalone behaviour. An interrupt (process shutdown) also
     * abandons the wait with an error log.
     */
    public static void parkBlocking(@Nonnull SegmentId segmentId, @Nonnull ParkBody body) {
        if (!SegmentStateSink.everInstalled()) {
            LOG.error(
                    "DLQ park impossible without a controller transport; dropping poison payload"
                            + " on channel `{}` after {} attempts: {}",
                    body.entry().channel(),
                    body.entry().attempts(),
                    body.entry().error());
            return;
        }
        byte[] frame = SegmentFlusher.buildSegmentStateChangeFrame(
                segmentId, PARK_SEQ.get(), new byte[16], encodeParkBody(body));

        long resends = 0;
        while (true) {
            SegmentStateSink.send(PAYLOAD_ID_PARK, frame);
            long deadline = System.currentTimeMillis() + ACK_RESEND_INTERVAL_MS;
            synchronized (ACK_LOCK) {
                while (true) {
                    if (acked.remove(body.parkId())) {
                        return;
                    }
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        break;
                    }
                    try {
                        ACK_LOCK.wait(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.error("DLQ park wait interrupted before ack; park `{}` may not be durable", body.parkId());
                        return;
                    }
                }
            }
            resends++;
            if (resends % WARN_EVERY_RESENDS == 0) {
                LOG.warn(
                        "dead-letter park `{}` on channel `{}` still unacked after {} re-sends;" + " segment held",
                        body.parkId(),
                        body.entry().channel(),
                        resends);
            }
        }
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        long v = value;
        while ((v & ~0x7FL) != 0) {
            out.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    private static void writeBytes(ByteArrayOutputStream out, byte[] bytes) {
        writeVarint(out, bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    private static void writeString(ByteArrayOutputStream out, String s) {
        writeBytes(out, s.getBytes(StandardCharsets.UTF_8));
    }
}
