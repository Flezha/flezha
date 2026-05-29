package com.flezha.sdk.runtime.state;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;

/**
 * Parser for the controller's on-disk WAL change-log framing, shipped verbatim
 * in {@code RestoreInitState.change_log_bytes}. Mirrors the Rust SDK's
 * {@code iter_change_log_records} and the controller's
 * {@code Flezha.State.WalWriter.encode_record/3}.
 *
 * <p>Each record is:
 * <pre>
 *   [seq: u64 BE][len: u32 BE][header: 16][change_bytes][crc32: u32 BE]
 * </pre>
 * where {@code len = 16 + change_bytes.length} and {@code crc32} covers
 * {@code header ++ change_bytes}. {@link Record#changeBytes()} excludes the
 * 16-byte header (exposed separately as {@link Record#header()}).
 */
public final class ChangeLogReader {

    private static final int HEADER_SIZE = 16;
    /** seq(8) + len(4) + header(16) + crc(4). */
    private static final int MIN_RECORD = 8 + 4 + HEADER_SIZE + 4;

    private ChangeLogReader() {}

    /** One framed WAL record. */
    public record Record(long seq, byte[] header, byte[] changeBytes) {}

    /**
     * Parse all concatenated records, validating the 16-byte header (via
     * {@link HeaderTemplate#decodeHeader}: {@code sdk_lang}/{@code format_id})
     * and the trailing crc32 over {@code header ++ change_bytes}.
     *
     * @throws StateLoadException on truncation, bad length, header mismatch, or
     *     crc mismatch.
     */
    @Nonnull
    public static List<Record> parse(@Nonnull byte[] bytes, @Nonnull HeaderTemplate template) {
        List<Record> records = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        while (buf.remaining() > 0) {
            if (buf.remaining() < MIN_RECORD) {
                throw new StateLoadException(
                        StateLoadException.Reason.TRUNCATED,
                        "change-log record truncated: " + buf.remaining() + " bytes remaining");
            }
            long seq = buf.getLong();
            long len = buf.getInt() & 0xFFFFFFFFL;
            if (len < HEADER_SIZE) {
                throw new StateLoadException(
                        StateLoadException.Reason.TRUNCATED, "change-log record len " + len + " < " + HEADER_SIZE);
            }
            int changeLen = (int) (len - HEADER_SIZE);
            if (buf.remaining() < HEADER_SIZE + changeLen + 4) {
                throw new StateLoadException(
                        StateLoadException.Reason.TRUNCATED, "change-log record body truncated at seq " + seq);
            }
            byte[] header = new byte[HEADER_SIZE];
            buf.get(header);
            byte[] changeBytes = new byte[changeLen];
            buf.get(changeBytes);
            int trailingCrc = buf.getInt();

            // Validate the header (sdk_lang == Java, format_id == Kryo); throws on mismatch.
            template.decodeHeader(header);

            CRC32 crc = new CRC32();
            crc.update(header);
            crc.update(changeBytes);
            if ((int) crc.getValue() != trailingCrc) {
                throw new StateLoadException(
                        StateLoadException.Reason.CRC_MISMATCH, "change-log crc mismatch at seq " + seq);
            }
            records.add(new Record(seq, header, changeBytes));
        }
        return records;
    }

    /** Highest seq across {@code records}, or 0 when empty (matches the Rust terminal seq). */
    public static long terminalSeq(@Nonnull List<Record> records) {
        long terminal = 0L;
        for (Record r : records) {
            terminal = Math.max(terminal, r.seq());
        }
        return terminal;
    }
}
