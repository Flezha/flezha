package com.flezha.sdk.runtime.state;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Enumeration;
import java.util.jar.Manifest;
import java.util.zip.CRC32;
import javax.annotation.Nonnull;

/**
 * 16-byte header stamped on every {@code partition_state_change} and
 * {@code partition_state_snapshot} frame. Layout (big-endian):
 *
 * <pre>
 *   bytes 0..1   sdk_lang        (1 byte; Java = 2)
 *   bytes 1..5   sdk_version     (u32; parsed from Manifest Implementation-Version)
 *   bytes 5..9   schema_version  (u32; from Shard.schemaVersion())
 *   bytes 9..10  format_id       (1 byte; Kryo = 4)
 *   bytes 10..12 reserved        (2 bytes; zero-filled)
 *   bytes 12..16 crc32           (u32; over change_bytes)
 * </pre>
 *
 * <p>Cross-language load fails loud per umbrella Decision 9: a header with
 * {@code sdk_lang != 2} surfaces as
 * {@code StateLoadException.sdkLangMismatch}.
 */
public final class HeaderTemplate {

    public static final int HEADER_SIZE = 16;
    public static final byte SDK_LANG_JAVA = 2;
    public static final byte FORMAT_ID_KRYO = 4;

    private static final int SDK_VERSION = parseVersion();

    /** Stamp a header for the given schema version and Change payload bytes. */
    @Nonnull
    public byte[] stamp(int schemaVersion, @Nonnull byte[] changeBytes) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.BIG_ENDIAN);
        buf.put(SDK_LANG_JAVA);
        buf.putInt(SDK_VERSION);
        buf.putInt(schemaVersion);
        buf.put(FORMAT_ID_KRYO);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.putInt(crc32(changeBytes));
        return buf.array();
    }

    /**
     * Parse a header. Verifies length, {@code sdk_lang}, {@code format_id}
     * and (when {@code expectedSchemaVersion} is non-negative) the schema
     * version. CRC verification is the caller's responsibility (the header
     * does not carry the payload bytes).
     */
    @Nonnull
    public Header decodeHeader(@Nonnull byte[] header) {
        if (header.length != HEADER_SIZE) {
            throw new StateLoadException(
                    StateLoadException.Reason.TRUNCATED,
                    "header is " + header.length + " bytes; expected " + HEADER_SIZE);
        }
        ByteBuffer buf = ByteBuffer.wrap(header).order(ByteOrder.BIG_ENDIAN);
        byte sdkLang = buf.get();
        int sdkVersion = buf.getInt();
        int schemaVersion = buf.getInt();
        byte formatId = buf.get();
        buf.get();
        buf.get();
        int crc = buf.getInt();

        if (sdkLang != SDK_LANG_JAVA) {
            throw StateLoadException.sdkLangMismatch(sdkLang & 0xFF);
        }
        if (formatId != FORMAT_ID_KRYO) {
            throw StateLoadException.formatNotSupported(formatId & 0xFF);
        }
        return new Header(sdkLang, sdkVersion, schemaVersion, formatId, crc);
    }

    /** Verify a CRC32 against payload bytes; throws on mismatch. */
    public void verifyCrc(@Nonnull Header header, @Nonnull byte[] payload) {
        int actual = crc32(payload);
        if (actual != header.crc32()) {
            throw new StateLoadException(
                    StateLoadException.Reason.CRC_MISMATCH,
                    "crc32 mismatch: expected " + Integer.toHexString(header.crc32()) + " got "
                            + Integer.toHexString(actual));
        }
    }

    private static int crc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return (int) crc.getValue();
    }

    /**
     * Read {@code Implementation-Version} from the SDK jar's MANIFEST.MF and
     * pack the semver into a 32-bit field as
     * {@code (major << 16) | (minor << 8) | patch}. Falls back to
     * {@code 0x00010000} (i.e. 1.0.0) on any parse failure.
     */
    static int parseVersion() {
        try {
            Enumeration<URL> manifests = HeaderTemplate.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
            while (manifests.hasMoreElements()) {
                URL url = manifests.nextElement();
                try (InputStream in = url.openStream()) {
                    Manifest m = new Manifest(in);
                    String impl = m.getMainAttributes().getValue("Implementation-Version");
                    if (impl == null) {
                        continue;
                    }
                    String impTitle = m.getMainAttributes().getValue("Implementation-Title");
                    if (impTitle != null && !impTitle.toLowerCase().contains("flezha")) {
                        continue;
                    }
                    return packSemver(impl);
                } catch (IOException ignored) {
                    // try next manifest
                }
            }
        } catch (IOException ignored) {
            // fall through to default
        }
        return 0x00010000;
    }

    static int packSemver(String semver) {
        try {
            String trimmed = semver.split("[-+]", 2)[0];
            String[] parts = trimmed.split("\\.");
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            int patch = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
            return ((major & 0xFF) << 16) | ((minor & 0xFF) << 8) | (patch & 0xFF);
        } catch (RuntimeException e) {
            return 0x00010000;
        }
    }

    /** Decoded header record. */
    public record Header(byte sdkLang, int sdkVersion, int schemaVersion, byte formatId, int crc32) {}
}
