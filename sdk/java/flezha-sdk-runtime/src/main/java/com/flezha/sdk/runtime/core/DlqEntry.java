package com.flezha.sdk.runtime.core;

import com.flezha.sdk.runtime.state.Postcard;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Dead-letter entry carried inside an {@link ItemEffect}. Field order matches
 * the controller wire ({@code DlqEntryBody}) and {@code flz_core}'s
 * {@code DlqEntry} exactly. Normally minted by the core when its retry ladder
 * parks; the host only decodes/encodes it at the activation boundary.
 */
public record DlqEntry(
        @Nonnull String channel, @Nonnull String error, int attempts, @Nonnull Optional<byte[]> payload) {

    public static void encode(@Nonnull Postcard.Writer w, @Nonnull DlqEntry entry) {
        w.string(entry.channel())
                .string(entry.error())
                .varint(Integer.toUnsignedLong(entry.attempts()))
                .optionalBytes(entry.payload());
    }

    @Nonnull
    public static DlqEntry decode(@Nonnull Postcard.Reader r) {
        return new DlqEntry(r.string(), r.string(), Math.toIntExact(r.varint()), r.optionalBytes());
    }
}
