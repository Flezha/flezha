package com.flezha.sdk.runtime.state;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * Postcard codec: the cross-language wire contract with the Rust SDK's
 * {@code postcard::to_allocvec} / {@code postcard::from_bytes}.
 *
 * <p>Every structure the controller decodes from this SDK — the dead-letter
 * {@code ParkBody}, the effectively-once {@code EffectRecordBody}, the bounded
 * proc-journal entries and outbox items — is encoded with these primitives. The
 * format is field-order-sensitive and has no self-describing framing, so an
 * encoder and its Rust counterpart must agree field for field.
 *
 * <p>Encoding rules: integers are LEB128 varints; {@code Vec<u8>} and
 * {@code String} are a varint length followed by raw (UTF-8) bytes;
 * {@code Option<T>} is {@code 0x00} for none or {@code 0x01} followed by the
 * value; {@code Vec<T>} is a varint element count followed by the elements.
 */
public final class Postcard {

    private Postcard() {}

    /** Cursor over an encoded buffer. Decoding advances {@link #pos}. */
    public static final class Reader {
        private final byte[] buf;
        private int pos;

        public Reader(@Nonnull byte[] buf) {
            this.buf = buf;
        }

        public int pos() {
            return pos;
        }

        public boolean exhausted() {
            return pos >= buf.length;
        }

        public long varint() {
            long value = 0;
            int shift = 0;
            while (true) {
                if (pos >= buf.length) {
                    throw new IllegalArgumentException("postcard: truncated varint");
                }
                int b = buf[pos++] & 0xFF;
                value |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
                if (shift > 63) {
                    throw new IllegalArgumentException("postcard: varint overflow");
                }
            }
        }

        @Nonnull
        public byte[] bytes() {
            int len = Math.toIntExact(varint());
            if (pos + len > buf.length) {
                throw new IllegalArgumentException("postcard: truncated byte string");
            }
            byte[] out = new byte[len];
            System.arraycopy(buf, pos, out, 0, len);
            pos += len;
            return out;
        }

        @Nonnull
        public String string() {
            return new String(bytes(), StandardCharsets.UTF_8);
        }

        /** {@code bool} — one byte, {@code 0x00} false / {@code 0x01} true. */
        public boolean bool() {
            if (pos >= buf.length) {
                throw new IllegalArgumentException("postcard: truncated bool");
            }
            int b = buf[pos++] & 0xFF;
            if (b > 1) {
                throw new IllegalArgumentException("postcard: bad bool byte " + b);
            }
            return b == 1;
        }

        public boolean some() {
            if (pos >= buf.length) {
                throw new IllegalArgumentException("postcard: truncated option tag");
            }
            int tag = buf[pos++] & 0xFF;
            if (tag > 1) {
                throw new IllegalArgumentException("postcard: bad option tag " + tag);
            }
            return tag == 1;
        }

        @Nonnull
        public OptionalLong optionalVarint() {
            return some() ? OptionalLong.of(varint()) : OptionalLong.empty();
        }

        @Nonnull
        public Optional<byte[]> optionalBytes() {
            return some() ? Optional.of(bytes()) : Optional.empty();
        }

        /** Decode a {@code Vec<T>}: varint count, then that many elements. */
        @Nonnull
        public <T> List<T> vec(@Nonnull java.util.function.Function<Reader, T> element) {
            int count = Math.toIntExact(varint());
            List<T> out = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                out.add(element.apply(this));
            }
            return out;
        }
    }

    /** Accumulating encoder. */
    public static final class Writer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Nonnull
        public Writer varint(long value) {
            long v = value;
            while ((v & ~0x7FL) != 0) {
                out.write((int) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.write((int) v);
            return this;
        }

        @Nonnull
        public Writer bytes(@Nonnull byte[] value) {
            varint(value.length);
            out.write(value, 0, value.length);
            return this;
        }

        @Nonnull
        public Writer string(@Nonnull String value) {
            return bytes(value.getBytes(StandardCharsets.UTF_8));
        }

        /** {@code bool} — one byte, {@code 0x00} false / {@code 0x01} true. */
        @Nonnull
        public Writer bool(boolean value) {
            out.write(value ? 1 : 0);
            return this;
        }

        /** {@code Option<u64>} — {@code 0x00}, or {@code 0x01} + varint. */
        @Nonnull
        public Writer optionalVarint(@Nonnull OptionalLong value) {
            if (value.isPresent()) {
                out.write(1);
                varint(value.getAsLong());
            } else {
                out.write(0);
            }
            return this;
        }

        /** {@code Option<Vec<u8>>} — {@code 0x00}, or {@code 0x01} + bytes. */
        @Nonnull
        public Writer optionalBytes(@Nonnull Optional<byte[]> value) {
            if (value.isPresent()) {
                out.write(1);
                bytes(value.get());
            } else {
                out.write(0);
            }
            return this;
        }

        /** {@code Option<T>} — {@code 0x00}, or {@code 0x01} + the encoded value. */
        @Nonnull
        public <T> Writer optional(@Nonnull Optional<T> value, @Nonnull java.util.function.BiConsumer<Writer, T> body) {
            if (value.isPresent()) {
                out.write(1);
                body.accept(this, value.get());
            } else {
                out.write(0);
            }
            return this;
        }

        /** {@code Vec<T>} — varint element count, then each element. */
        @Nonnull
        public <T> Writer vec(@Nonnull List<T> values, @Nonnull java.util.function.BiConsumer<Writer, T> element) {
            varint(values.size());
            for (T value : values) {
                element.accept(this, value);
            }
            return this;
        }

        @Nonnull
        public byte[] toBytes() {
            return out.toByteArray();
        }
    }
}
