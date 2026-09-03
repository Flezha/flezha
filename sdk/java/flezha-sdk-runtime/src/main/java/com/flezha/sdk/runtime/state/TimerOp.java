package com.flezha.sdk.runtime.state;

import com.flezha.sdk.state.TimerKind;
import javax.annotation.Nonnull;

/**
 * One durable mutation of an Entity's pending-timer set.
 *
 * <p>Field order is the wire contract — postcard is field-order sensitive, and
 * this must match the Rust SDK's {@code TimerOp} and the controller's
 * {@code Flezha.State.Wire} decoder exactly. Pinned by {@code TimerOpTest}
 * against the same fixture the Rust side asserts.
 *
 * <p>Ops ride the WAL inside {@link EffectRecord#timers()}, which the
 * controller stores verbatim, so a firing's effects and the removal of the
 * timer that produced them land in ONE record.
 */
public record TimerOp(@Nonnull TimerKind kind, long scheduledAt, boolean register) {

    @Nonnull
    public static TimerOp register(@Nonnull TimerKind kind, long scheduledAt) {
        return new TimerOp(kind, scheduledAt, true);
    }

    @Nonnull
    public static TimerOp cancel(@Nonnull TimerKind kind, long scheduledAt) {
        return new TimerOp(kind, scheduledAt, false);
    }

    public static void encode(@Nonnull Postcard.Writer w, @Nonnull TimerOp op) {
        w.varint(op.kind().ordinal()).varint(op.scheduledAt()).bool(op.register());
    }

    @Nonnull
    public static TimerOp decode(@Nonnull Postcard.Reader r) {
        int ordinal = Math.toIntExact(r.varint());
        TimerKind[] kinds = TimerKind.values();
        if (ordinal < 0 || ordinal >= kinds.length) {
            throw new IllegalArgumentException("postcard: unknown TimerKind ordinal " + ordinal);
        }
        return new TimerOp(kinds[ordinal], r.varint(), r.bool());
    }
}
