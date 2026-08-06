package com.flezha.sdk.runtime.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flezha.sdk.state.Change;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Byte-level conformance of the SDK→controller per-Entity wire framing and
 * the 0-based change-seq, plus the WAL change-log parser. These must match the
 * controller's {@code Flezha.State.Wire}/{@code WalWriter} byte-for-byte and the
 * Rust SDK's behaviour.
 */
class EntityStateWireTest {

    /** Minimal changeful payload for codec round-trips. */
    public record AddChange(long delta) implements Change {}

    @BeforeEach
    @AfterEach
    void reset() {
        EntityFlusher.resetForTesting();
        EntityStateSink.resetForTesting();
    }

    @Test
    void encodedBytesMatchesControllerFraming() {
        // [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
        byte[] id = new EntityId("MarketUnboundedOperative", "OhlcProcedure", "M1").encodedBytes();
        assertThat(id[0]).isEqualTo((byte) 24);
        assertThat(new String(id, 1, 24, StandardCharsets.UTF_8)).isEqualTo("MarketUnboundedOperative");
        assertThat(id[25]).isEqualTo((byte) 13);
        assertThat(new String(id, 26, 13, StandardCharsets.UTF_8)).isEqualTo("OhlcProcedure");
        ByteBuffer pk = ByteBuffer.wrap(id, 39, 4).order(ByteOrder.BIG_ENDIAN);
        assertThat(pk.getShort() & 0xFFFF).isEqualTo(2);
        assertThat(new String(id, 41, 2, StandardCharsets.UTF_8)).isEqualTo("M1");
        assertThat(id.length).isEqualTo(43);
    }

    @Test
    void frameLayoutMatchesControllerDecodeFormat() {
        KryoCodec codec = new KryoCodec(List.of(AddChange.class));
        HeaderTemplate template = new HeaderTemplate();
        byte[] body = codec.serialize(new AddChange(42));
        byte[] header = template.stamp(1, body);
        EntityId pid = new EntityId("MarketUnboundedOperative", "OhlcProcedure", "M1");

        byte[] frame = EntityFlusher.buildEntityStateChangeFrame(pid, 99L, header, body);

        ByteBuffer buf = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
        int pidLen = buf.getShort() & 0xFFFF;
        assertThat(pidLen).isEqualTo(pid.encodedBytes().length);
        byte[] pidBytes = new byte[pidLen];
        buf.get(pidBytes);
        assertThat(pidBytes).isEqualTo(pid.encodedBytes());
        assertThat(buf.getLong()).isEqualTo(99L);
        byte[] gotHeader = new byte[16];
        buf.get(gotHeader);
        assertThat(gotHeader).isEqualTo(header);
        int bodyLen = buf.getInt();
        assertThat(bodyLen).isEqualTo(body.length);
        byte[] gotBody = new byte[bodyLen];
        buf.get(gotBody);
        assertThat(gotBody).isEqualTo(body);
        // Trailing crc covers the body (change_bytes) ONLY.
        CRC32 crc = new CRC32();
        crc.update(body);
        assertThat(buf.getInt()).isEqualTo((int) crc.getValue());
        assertThat(buf.remaining()).isZero();
    }

    @Test
    void changeLogReaderRoundTripsAndValidatesCrc() {
        KryoCodec codec = new KryoCodec(List.of(AddChange.class));
        HeaderTemplate template = new HeaderTemplate();
        byte[] body = codec.serialize(new AddChange(7));
        byte[] header = template.stamp(1, body);
        byte[] record = walRecord(3L, header, body);

        List<ChangeLogReader.Record> records = ChangeLogReader.parse(record, template);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).seq()).isEqualTo(3L);
        AddChange decoded = (AddChange) codec.deserialize(records.get(0).changeBytes(), Change.class);
        assertThat(decoded.delta()).isEqualTo(7L);

        // A flipped crc byte must fail validation.
        byte[] corrupt = record.clone();
        corrupt[corrupt.length - 1] ^= 0xFF;
        assertThatThrownBy(() -> ChangeLogReader.parse(corrupt, template)).isInstanceOf(StateLoadException.class);
    }

    @Test
    void flusherEmitsFirstChangeAtSeqZero() {
        List<byte[]> captured = new ArrayList<>();
        EntityStateSink.installGlobalSink((payloadId, frame) -> {
            captured.add(frame);
            return true;
        });

        KryoCodec codec = new KryoCodec(List.of(AddChange.class));
        EntityId pid = new EntityId("inst-1", "counter_procedure", "");
        Object lock = new Object();
        List<Change> buffer = new ArrayList<>();
        AtomicLong seq = new AtomicLong(0L);
        FlusherEntry entry = new FlusherEntry(
                lock, buffer, List.of(), true, seq, EntityStateSink::send, new HeaderTemplate(), pid, 1, codec);

        EntityFlusher flusher = EntityFlusher.getOrStart();
        flusher.register(pid, entry, AddChange.class);
        synchronized (lock) {
            buffer.add(new AddChange(5));
        }
        flusher.drainOnce();

        assertThat(captured).hasSize(1);
        ByteBuffer buf = ByteBuffer.wrap(captured.get(0)).order(ByteOrder.BIG_ENDIAN);
        int pidLen = buf.getShort() & 0xFFFF;
        buf.position(buf.position() + pidLen);
        assertThat(buf.getLong()).as("first emitted change must carry seq 0").isEqualTo(0L);
        assertThat(seq.get()).as("changeSeq advances to 1 after the first emit").isEqualTo(1L);
    }

    /** Synthesize one controller-format WAL record (mirrors WalWriter.encode_record). */
    static byte[] walRecord(long seq, byte[] header, byte[] changeBytes) {
        int len = 16 + changeBytes.length;
        CRC32 crc = new CRC32();
        crc.update(header);
        crc.update(changeBytes);
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)) {
            out.writeLong(seq);
            out.writeInt(len);
            out.write(header);
            out.write(changeBytes);
            out.writeInt((int) crc.getValue());
            return bos.toByteArray();
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
