package com.flezha.sdk.runtime.core;

import com.flezha.sdk.runtime.state.TimerOp;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * The effect record of one processed {@link WorkItem} — the shape the flusher
 * drains today ({@code EffectRecordBody}), plus the verdict. {@code changes}
 * elements are host-encoded (Kryo) and core-opaque. {@code dlq} is normally
 * empty; the core mints the entry when its ladder parks.
 */
public record ItemEffect(
        @Nonnull Verdict verdict,
        @Nonnull List<byte[]> changes,
        @Nonnull List<Output> outputs,
        @Nonnull OptionalLong consumedOffset,
        long eventTime,
        @Nonnull List<TimerOp> timers,
        boolean checkpoint,
        @Nonnull Optional<DlqEntry> dlq) {}
