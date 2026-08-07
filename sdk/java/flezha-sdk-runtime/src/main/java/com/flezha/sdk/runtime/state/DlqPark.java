package com.flezha.sdk.runtime.state;

import com.flezha.sdk.channel.RetryPolicy;
import com.flezha.sdk.runtime.control.AckRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dead-letter park: retry-exhausted payloads ship to the controller's
 * durable DLQ store and the entity advances only once the park is durable
 * ({@code dead-letter-queue}). Mirrors the Rust SDK's
 * {@code entity::park}: a {@code park_payload} data-stream frame on the
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

    private static final long ACK_RESEND_INTERVAL_MS = 2_000L;
    private static final long WARN_EVERY_RESENDS = 5L;

    private DlqPark() {}

    /**
     * The park's dead-letter record — the same shape a tier-on park embeds
     * in its effect record (Rust {@code DlqEntryBody}). {@code payload} is
     * empty when the input type is not byte-representable (inter-procedure
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
    public static String mintParkId(@Nonnull String entityLabel) {
        return entityLabel + "#" + PARK_SEQ.getAndIncrement();
    }

    /**
     * Record a controller ack for {@code parkId} and wake the parked
     * dispatcher. Called from the {@code ParkPayloadAck} control handler.
     */
    public static void completeAck(@Nonnull String parkId) {
        AckRegistry.complete(parkId);
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
     * Postcard encoding of a {@link ParkBody}, byte-identical to the Rust
     * SDK's {@code postcard::to_allocvec(ParkBody)}. Field order is the wire
     * contract.
     */
    @Nonnull
    public static byte[] encodeParkBody(@Nonnull ParkBody body) {
        Postcard.Writer w = new Postcard.Writer();
        w.string(body.parkId()).optionalVarint(body.originOffset());
        encodeDlqEntry(w, body.entry());
        return w.toBytes();
    }

    /**
     * Postcard encoding of a {@link DlqEntry} (Rust {@code DlqEntryBody}).
     * Shared with the effectively-once tier, whose effect record embeds the
     * same entry so that a park and its consumed-offset advance stay one
     * atomic WAL record.
     */
    public static void encodeDlqEntry(@Nonnull Postcard.Writer w, @Nonnull DlqEntry entry) {
        w.string(entry.channel())
                .string(entry.error())
                .varint(Integer.toUnsignedLong(entry.attempts()))
                .optionalBytes(entry.payload());
    }

    /** Inverse of {@link #encodeDlqEntry}; used by the effect-record decoder on restore. */
    @Nonnull
    public static DlqEntry decodeDlqEntry(@Nonnull Postcard.Reader r) {
        return new DlqEntry(r.string(), r.string(), Math.toIntExact(r.varint()), r.optionalBytes());
    }

    /**
     * Ship a park frame and block until the controller acks it as durable.
     * Re-sends on the resend cadence forever — the entity must not advance
     * past an unparked poison payload. Transport-less runs (no sink ever
     * installed) cannot park; they log loudly and drop, preserving the
     * pre-DLQ standalone behaviour. An interrupt (process shutdown) also
     * abandons the wait with an error log.
     */
    /** One re-attempt of the step a {@link #retryThenPark} ladder is climbing. */
    @FunctionalInterface
    public interface Step {
        /** Re-run and, on success, deliver. Throws to report another failure. */
        void run();
    }

    /**
     * Re-attempt one failed step off the routing thread per the channel's retry
     * budget, then park what still fails under {@code identity}. The one ladder
     * both poison entry payloads and unwinding edge closures climb — the step
     * re-runs AND delivers on success, so the two differ only in what that step
     * is. Mirror of the Rust SDK's {@code run_retry_park_ladder}.
     */
    public static void retryThenPark(
            @Nonnull String label,
            @Nonnull String channelName,
            @Nonnull RetryPolicy retry,
            @Nonnull EntityId identity,
            @Nonnull OptionalLong offset,
            @Nonnull Optional<byte[]> payload,
            @Nonnull String firstError,
            @Nonnull Step step) {
        int maxAttempts = Math.max(1, retry.maxAttempts());
        int attempt = 1; // the routing-thread run was attempt 1
        String error = firstError;
        while (attempt < maxAttempts) {
            RETRIES_TOTAL.incrementAndGet();
            LOG.warn("{} attempt {}/{} failed (retrying): {}", label, attempt, maxAttempts, error);
            sleepQuietly(retry.backoffFor(attempt));
            attempt++;
            try {
                step.run();
                return;
            } catch (RuntimeException e) {
                error = e.getMessage() != null ? e.getMessage() : e.toString();
            }
        }

        PARKED_TOTAL.incrementAndGet();
        LOG.error("{} parking payload to DLQ after {} attempt(s): {}", label, attempt, error);
        parkBlocking(
                identity,
                new ParkBody(
                        mintParkId(identity.encoded()), offset, new DlqEntry(channelName, error, attempt, payload)));
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void parkBlocking(@Nonnull EntityId entityId, @Nonnull ParkBody body) {
        if (!EntityStateSink.everInstalled()) {
            LOG.error(
                    "DLQ park impossible without a controller transport; dropping poison payload"
                            + " on channel `{}` after {} attempts: {}",
                    body.entry().channel(),
                    body.entry().attempts(),
                    body.entry().error());
            return;
        }
        byte[] frame =
                EntityFlusher.buildEntityStateChangeFrame(entityId, PARK_SEQ.get(), new byte[16], encodeParkBody(body));

        // Register before the first send: the ack can then never outrun the
        // waiter, which is what lets the shared registry stay race-free.
        CompletableFuture<String> ack = AckRegistry.register(body.parkId());
        long resends = 0;
        while (true) {
            EntityStateSink.send(PAYLOAD_ID_PARK, frame);
            if (AckRegistry.await(body.parkId(), ack, ACK_RESEND_INTERVAL_MS)) {
                return;
            }
            if (Thread.currentThread().isInterrupted()) {
                LOG.error("DLQ park wait interrupted before ack; park `{}` may not be durable", body.parkId());
                return;
            }
            resends++;
            if (resends % WARN_EVERY_RESENDS == 0) {
                LOG.warn(
                        "dead-letter park `{}` on channel `{}` still unacked after {} re-sends;" + " entity held",
                        body.parkId(),
                        body.entry().channel(),
                        resends);
            }
        }
    }
}
