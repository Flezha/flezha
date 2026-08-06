package com.flezha.sdk.runtime.state;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Shared warm-restart decode for an Entity's type-erased state, used by both the
 * unbounded ({@code RestoreInitStateHandler}) and bounded
 * ({@code BoundedOperativeRunner.BoundedEntity}) restore paths so the two can
 * never diverge on how persisted state is rebuilt.
 *
 * <p>User init seeds the state; a non-empty snapshot then <strong>replaces</strong>
 * it (the controller has already stripped the snapshot's 16-byte header); finally
 * the change-log folds on top via {@link Procedure#applyChange} — so
 * snapshot-covered Changes are never re-applied. The caller installs the rebuilt
 * state and re-arms its change-seq; this helper is pure (no lock, no slot).
 */
public final class EntityStateRestore {

    private EntityStateRestore() {}

    /** Rebuilt state plus the terminal (highest) WAL seq (0 when the change-log is empty). */
    public record Rebuilt(@Nonnull Object state, long terminalSeq) {}

    /**
     * Rebuild an Entity's state from the shipped {@code snapshot} + {@code changeLog}.
     * {@code procedure} / {@code codec} / {@code entityKey} come from the entity
     * being restored.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Rebuilt rebuild(
            @Nonnull Procedure procedure,
            @Nonnull KryoCodec codec,
            @Nonnull Optional<String> entityKey,
            @Nonnull byte[] snapshot,
            @Nonnull byte[] changeLog,
            @Nonnull HeaderTemplate headerTemplate) {
        Object state = procedure.initState(entityKey, Optional.of(new RestoreInitPayload(snapshot, changeLog)));
        if (snapshot.length > 0) {
            state = codec.deserialize(snapshot, Object.class);
        }
        List<ChangeLogReader.Record> records = ChangeLogReader.parse(changeLog, headerTemplate);
        for (ChangeLogReader.Record record : records) {
            Change change = codec.deserialize(record.changeBytes(), Change.class);
            procedure.applyChange(state, change);
        }
        return new Rebuilt(state, ChangeLogReader.terminalSeq(records));
    }
}
