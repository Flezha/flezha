package com.flezha.sdk.runtime.framing;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Resumable frame parser. Bytes may arrive in arbitrary chunks and the
 * parser tracks its own state across calls.
 *
 * <p>Internal states:
 * <ul>
 *   <li>EMPTY — no bytes buffered.</li>
 *   <li>INCOMPLETE_HEADER — between 1 and 35 header bytes
 *       (header is 32 bytes payload id + 4 bytes payload size = 36).</li>
 *   <li>INCOMPLETE_PAYLOAD — header parsed; waiting for payload bytes.</li>
 * </ul>
 *
 * <p>Not thread-safe; each stream owns its own parser.
 *
 * <p>See {@code docs/sdk_protocol_spec.md} §3.
 */
public final class FrameParser {

    private static final int HEADER_SIZE = FrameEncoder.PAYLOAD_ID_WIDTH + 4;

    private enum State {
        EMPTY,
        INCOMPLETE_HEADER,
        INCOMPLETE_PAYLOAD
    }

    private State state = State.EMPTY;
    private final Deque<byte[]> buffered = new ArrayDeque<>();
    private int bufferedLen = 0;

    private String currentPayloadId;
    private int currentPayloadSize;

    /**
     * Feed the parser a chunk. Returns all frames that became complete after
     * this input. Remaining bytes are retained internally for the next call.
     */
    @Nonnull
    public List<Frame> feed(@Nonnull byte[] chunk) {
        if (chunk == null) {
            throw new IllegalArgumentException("chunk must not be null");
        }
        return feed(chunk, 0, chunk.length);
    }

    @Nonnull
    @SuppressWarnings("fallthrough") // INCOMPLETE_HEADER falls through to INCOMPLETE_PAYLOAD by design once the header is parsed
    public List<Frame> feed(@Nonnull byte[] chunk, int offset, int length) {
        if (chunk == null) {
            throw new IllegalArgumentException("chunk must not be null");
        }
        if (offset < 0 || length < 0 || offset + length > chunk.length) {
            throw new IllegalArgumentException(
                    "chunk offset/length out of bounds: offset=" + offset
                            + " length=" + length + " array=" + chunk.length);
        }

        if (length > 0) {
            byte[] copy = new byte[length];
            System.arraycopy(chunk, offset, copy, 0, length);
            buffered.addLast(copy);
            bufferedLen += length;
        }

        List<Frame> out = new ArrayList<>();
        while (true) {
            switch (state) {
                case EMPTY:
                case INCOMPLETE_HEADER:
                    if (bufferedLen < HEADER_SIZE) {
                        state = State.INCOMPLETE_HEADER;
                        return out;
                    }
                    parseHeader();
                    state = State.INCOMPLETE_PAYLOAD;
                    // $FALL-THROUGH$
                case INCOMPLETE_PAYLOAD:
                    if (bufferedLen < currentPayloadSize) {
                        return out;
                    }
                    Frame frame = parsePayload();
                    out.add(frame);
                    state = bufferedLen == 0 ? State.EMPTY : State.INCOMPLETE_HEADER;
                    break;
            }
        }
    }

    public boolean hasPending() {
        return bufferedLen > 0;
    }

    int bufferedBytes() {
        return bufferedLen;
    }

    private void parseHeader() {
        byte[] header = takeBytes(HEADER_SIZE);
        currentPayloadId = new String(header, 0, FrameEncoder.PAYLOAD_ID_WIDTH, StandardCharsets.UTF_8).trim();
        ByteBuffer bb = ByteBuffer.wrap(header, FrameEncoder.PAYLOAD_ID_WIDTH, 4).order(ByteOrder.BIG_ENDIAN);
        currentPayloadSize = bb.getInt();
        if (currentPayloadSize < 6) {
            throw new IllegalStateException(
                    "Invalid frame: payload size " + currentPayloadSize
                            + " is less than the minimum 6 (message_size u32 + metadata_size u16)");
        }
    }

    @Nonnull
    private Frame parsePayload() {
        byte[] payload = takeBytes(currentPayloadSize);

        ByteBuffer bb = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int messageSize = bb.getInt();
        if (messageSize < 0 || messageSize > currentPayloadSize - 6) {
            throw new IllegalStateException(
                    "Invalid frame: message size " + messageSize
                            + " exceeds payload bounds");
        }
        byte[] message = new byte[messageSize];
        bb.get(message);

        int metadataSize = bb.getShort() & 0xFFFF;
        if (metadataSize > currentPayloadSize - 6 - messageSize) {
            throw new IllegalStateException(
                    "Invalid frame: metadata size " + metadataSize
                            + " exceeds payload bounds");
        }
        byte[] metadataBytes = new byte[metadataSize];
        bb.get(metadataBytes);

        Map<String, String> metadata = MetadataCodec.deserialize(metadataBytes);

        return new Frame(currentPayloadId, message, metadata);
    }

    @Nonnull
    private byte[] takeBytes(int n) {
        byte[] out = new byte[n];
        int written = 0;
        while (written < n) {
            byte[] head = buffered.peekFirst();
            int headRemaining = head.length;
            int take = Math.min(headRemaining, n - written);
            System.arraycopy(head, 0, out, written, take);
            written += take;

            if (take == headRemaining) {
                buffered.pollFirst();
            } else {
                byte[] newHead = new byte[headRemaining - take];
                System.arraycopy(head, take, newHead, 0, newHead.length);
                buffered.pollFirst();
                buffered.addFirst(newHead);
            }
        }
        bufferedLen -= n;
        return out;
    }
}
