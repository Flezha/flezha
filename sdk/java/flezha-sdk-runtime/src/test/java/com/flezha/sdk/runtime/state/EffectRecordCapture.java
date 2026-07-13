package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Captures the effect records a Segment actually ships.
 *
 * <p>Tests must NOT assert on {@code SegmentSlot.effectBuffer()}: the background
 * flusher drains it within a linger tick (~5 ms), so such an assertion races and
 * flakes under load. Assert on what reached the sink instead — which is stronger
 * anyway, since it exercises the frame and its header too.
 */
public final class EffectRecordCapture {

    private final List<Map.Entry<String, byte[]>> sends = new CopyOnWriteArrayList<>();

    /** Install as the global state sink. Call from {@code @BeforeEach}. */
    public void install() {
        sends.clear();
        SegmentStateSink.installGlobalSink((payloadId, frame) -> {
            sends.add(Map.entry(payloadId, frame));
            return true;
        });
    }

    /** Block until at least {@code n} effect records have shipped, or time out. */
    public void awaitRecords(int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (records().size() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(5L);
        }
    }

    /** Every shipped effect record, in ship order. */
    public List<EffectRecord> records() {
        List<EffectRecord> out = new ArrayList<>();
        for (Map.Entry<String, byte[]> sent : sends) {
            if (SegmentStateSink.PAYLOAD_ID_EFFECT_RECORD.equals(sent.getKey())) {
                out.add(decode(sent.getValue()));
            }
        }
        return out;
    }

    /** The single effect record shipped. Fails if there is not exactly one. */
    public EffectRecord only() {
        List<EffectRecord> all = records();
        assertThat(all).hasSize(1);
        return all.get(0);
    }

    /** Whether any frame shipped under {@code payloadId}. */
    public boolean shipped(String payloadId) {
        return sends.stream().anyMatch(e -> e.getKey().equals(payloadId));
    }

    /**
     * Frame layout: {@code [u16 idLen][id][u64 seq][16B header][u32 len][body][u32 crc]}.
     * Asserts the header's format id — an effect record and a plain change record
     * share this frame shape and are told apart only by that byte.
     */
    private static EffectRecord decode(byte[] frame) {
        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
        int idLen = Short.toUnsignedInt(buf.getShort());
        buf.position(buf.position() + idLen);
        buf.getLong(); // seq

        byte[] header = new byte[HeaderTemplate.HEADER_SIZE];
        buf.get(header);
        assertThat(header[0]).as("sdk_lang = Java").isEqualTo(HeaderTemplate.SDK_LANG_JAVA);
        assertThat(header[9]).as("format_id = effect record").isEqualTo(HeaderTemplate.FORMAT_ID_EFFECT_RECORD);

        byte[] body = new byte[buf.getInt()];
        buf.get(body);

        Postcard.Reader r = new Postcard.Reader(body);
        List<byte[]> changes = r.vec(Postcard.Reader::bytes);
        List<EffectRecord.OutputRecord> outputs = r.vec(x -> {
            String channel = x.string();
            String key = x.string();
            List<Map.Entry<String, String>> meta = x.vec(m -> Map.entry(m.string(), m.string()));
            return new EffectRecord.OutputRecord(channel, key, meta, x.bytes());
        });
        OptionalLong offset = r.optionalVarint();
        Optional<DlqPark.DlqEntry> dlq = r.some()
                ? Optional.of(new DlqPark.DlqEntry(r.string(), r.string(), (int) r.varint(), r.optionalBytes()))
                : Optional.empty();
        return new EffectRecord(changes, outputs, offset, dlq, r.varint());
    }
}
