package com.flezha.sdk.runtime.state;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Shared warm-restart decode for a Segment's type-erased state, used by both the
 * continuous ({@code RestoreInitStateHandler}) and bounded
 * ({@code BoundedOperativeRunner.BoundedSegment}) restore paths so the two can
 * never diverge on how persisted state is rebuilt.
 *
 * <p>User init seeds the state; a non-empty snapshot then <strong>replaces</strong>
 * it (the controller has already stripped the snapshot's 16-byte header); finally
 * the change-log folds on top via {@link Operation#applyChange} — so
 * snapshot-covered Changes are never re-applied. The caller installs the rebuilt
 * state and re-arms its change-seq; this helper is pure (no lock, no slot).
 */
public final class SegmentStateRestore {

    private SegmentStateRestore() {}

    /** Rebuilt state plus the terminal (highest) WAL seq (0 when the change-log is empty). */
    public record Rebuilt(@Nonnull Object state, long terminalSeq) {}

    /**
     * Rebuild a Segment's state from the shipped {@code snapshot} + {@code changeLog}.
     * {@code operation} / {@code codec} / {@code operationKey} come from the segment
     * being restored.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Rebuilt rebuild(
            @Nonnull Operation operation,
            @Nonnull KryoCodec codec,
            @Nonnull Optional<String> operationKey,
            @Nonnull byte[] snapshot,
            @Nonnull byte[] changeLog,
            @Nonnull HeaderTemplate headerTemplate) {
        Object state = operation.initState(operationKey, Optional.of(new RestoreInitPayload(snapshot, changeLog)));
        if (snapshot.length > 0) {
            state = codec.deserialize(snapshot, Object.class);
        }
        List<ChangeLogReader.Record> records = ChangeLogReader.parse(changeLog, headerTemplate);
        for (ChangeLogReader.Record record : records) {
            Change change = codec.deserialize(record.changeBytes(), Change.class);
            operation.applyChange(state, change);
        }
        return new Rebuilt(state, ChangeLogReader.terminalSeq(records));
    }
}
