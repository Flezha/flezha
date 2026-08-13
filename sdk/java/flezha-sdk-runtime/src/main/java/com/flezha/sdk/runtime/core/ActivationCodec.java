package com.flezha.sdk.runtime.core;

import com.flezha.sdk.runtime.state.Postcard;
import com.flezha.sdk.runtime.state.TimerOp;
import com.flezha.sdk.state.TimerKind;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Postcard codec for the poll-core activation boundary (schema version 2),
 * byte-identical to {@code flz_core::schema}. Both directions exist for every
 * type: the shim decodes Activations and encodes ActivationResults on the hot
 * path; the inverses back the cross-language fixtures.
 */
public final class ActivationCodec {

    private ActivationCodec() {}

    // ------------------------------------------------------------------
    // Activation
    // ------------------------------------------------------------------

    @Nonnull
    public static Activation decodeActivation(@Nonnull byte[] bytes) {
        Postcard.Reader r = new Postcard.Reader(bytes);
        Activation a = new Activation(r.varint(), decodeEntityRef(r), decodeKind(r), r.varint(), r.string());
        if (!r.exhausted()) {
            throw new IllegalArgumentException("postcard: trailing bytes after Activation at " + r.pos());
        }
        return a;
    }

    @Nonnull
    public static byte[] encodeActivation(@Nonnull Activation a) {
        Postcard.Writer w = new Postcard.Writer();
        w.varint(a.token());
        encodeEntityRef(w, a.target());
        encodeKind(w, a.kind());
        w.varint(a.changeSeq()).string(a.factory());
        return w.toBytes();
    }

    // ------------------------------------------------------------------
    // ActivationResult
    // ------------------------------------------------------------------

    @Nonnull
    public static byte[] encodeResult(@Nonnull ActivationResult result) {
        Postcard.Writer w = new Postcard.Writer();
        w.varint(result.token());
        encodeOutcome(w, result.outcome());
        return w.toBytes();
    }

    @Nonnull
    public static ActivationResult decodeResult(@Nonnull byte[] bytes) {
        Postcard.Reader r = new Postcard.Reader(bytes);
        ActivationResult result = new ActivationResult(r.varint(), decodeOutcome(r));
        if (!r.exhausted()) {
            throw new IllegalArgumentException("postcard: trailing bytes after ActivationResult at " + r.pos());
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Activation internals
    // ------------------------------------------------------------------

    private static void encodeEntityRef(Postcard.Writer w, EntityRef ref) {
        w.string(ref.operative()).varint(Integer.toUnsignedLong(ref.procedure()));
        w.optional(ref.entityKey(), Postcard.Writer::string);
    }

    private static EntityRef decodeEntityRef(Postcard.Reader r) {
        return new EntityRef(r.string(), Math.toIntExact(r.varint()), optionalString(r));
    }

    private static void encodeKind(Postcard.Writer w, ActivationKind kind) {
        switch (kind) {
            case ActivationKind.Work work -> w.varint(0).vec(work.items(), ActivationCodec::encodeWorkItem);
            case ActivationKind.RestoreFold fold -> {
                w.varint(1).optionalBytes(fold.snapshot());
                w.vec(fold.changes(), Postcard.Writer::bytes);
            }
            case ActivationKind.SnapshotRequest ignored -> w.varint(2);
            case ActivationKind.LiveStateRead ignored -> w.varint(3);
            case ActivationKind.ApplyState apply -> w.varint(4).bytes(apply.stateJson());
            case ActivationKind.MigrationSerialize ignored -> w.varint(5);
            case ActivationKind.KeyRequest request -> {
                w.varint(6).varint(Integer.toUnsignedLong(request.channel()));
                w.vec(request.payloads(), Postcard.Writer::bytes)
                        .bool(request.wantKeys())
                        .bool(request.wantEventTime());
            }
            case ActivationKind.WorkflowRun run -> w.varint(7).bytes(run.args());
            case ActivationKind.CompensateRun comp -> w.varint(8).vec(comp.outputs(), Postcard.Writer::bytes);
        }
    }

    private static ActivationKind decodeKind(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        return switch (discriminant) {
            case 0 -> new ActivationKind.Work(r.vec(ActivationCodec::decodeWorkItem));
            case 1 -> new ActivationKind.RestoreFold(r.optionalBytes(), r.vec(Postcard.Reader::bytes));
            case 2 -> new ActivationKind.SnapshotRequest();
            case 3 -> new ActivationKind.LiveStateRead();
            case 4 -> new ActivationKind.ApplyState(r.bytes());
            case 5 -> new ActivationKind.MigrationSerialize();
            case 6 -> new ActivationKind.KeyRequest(
                    Math.toIntExact(r.varint()), r.vec(Postcard.Reader::bytes), r.bool(), r.bool());
            case 7 -> new ActivationKind.WorkflowRun(r.bytes());
            case 8 -> new ActivationKind.CompensateRun(r.vec(Postcard.Reader::bytes));
            default -> throw new IllegalArgumentException(
                    "postcard: unknown ActivationKind discriminant " + discriminant);
        };
    }

    private static void encodeWorkItem(Postcard.Writer w, WorkItem item) {
        switch (item) {
            case WorkItem.Input input -> {
                w.varint(0).varint(Integer.toUnsignedLong(input.channel())).bytes(input.payload());
                w.vec(input.metadata(), ActivationCodec::encodeMetadataPair);
                w.optionalVarint(input.consumedOffset()).bool(input.replay()).varint(input.now());
            }
            case WorkItem.Timer timer -> w.varint(1)
                    .varint(timer.kind().ordinal())
                    .varint(timer.scheduledAt());
        }
    }

    private static WorkItem decodeWorkItem(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        return switch (discriminant) {
            case 0 -> new WorkItem.Input(
                    Math.toIntExact(r.varint()),
                    r.bytes(),
                    r.vec(m -> Map.entry(m.string(), m.string())),
                    r.optionalVarint(),
                    r.bool(),
                    r.varint());
            case 1 -> new WorkItem.Timer(timerKind(r), r.varint());
            default -> throw new IllegalArgumentException("postcard: unknown WorkItem discriminant " + discriminant);
        };
    }

    // ------------------------------------------------------------------
    // ActivationResult internals
    // ------------------------------------------------------------------

    private static void encodeOutcome(Postcard.Writer w, ActivationOutcome outcome) {
        switch (outcome) {
            case ActivationOutcome.Work work -> w.varint(0).vec(work.effects(), ActivationCodec::encodeItemEffect);
            case ActivationOutcome.Restored ignored -> w.varint(1);
            case ActivationOutcome.Snapshot snapshot -> w.varint(2).bytes(snapshot.state());
            case ActivationOutcome.StateJson state -> w.varint(3).bytes(state.json());
            case ActivationOutcome.StateApplied ignored -> w.varint(4);
            case ActivationOutcome.MigrationState state -> w.varint(5).bytes(state.json());
            case ActivationOutcome.Prepared prepared -> w.varint(6)
                    .vec(prepared.answers(), ActivationCodec::encodePrepareResult);
            case ActivationOutcome.Failed failed -> {
                w.varint(7);
                encodeWorkError(w, failed.error());
            }
            case ActivationOutcome.WorkflowDone done -> w.varint(8)
                    .bool(done.completed())
                    .bytes(done.result());
        }
    }

    private static ActivationOutcome decodeOutcome(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        return switch (discriminant) {
            case 0 -> new ActivationOutcome.Work(r.vec(ActivationCodec::decodeItemEffect));
            case 1 -> new ActivationOutcome.Restored();
            case 2 -> new ActivationOutcome.Snapshot(r.bytes());
            case 3 -> new ActivationOutcome.StateJson(r.bytes());
            case 4 -> new ActivationOutcome.StateApplied();
            case 5 -> new ActivationOutcome.MigrationState(r.bytes());
            case 6 -> new ActivationOutcome.Prepared(r.vec(ActivationCodec::decodePrepareResult));
            case 7 -> new ActivationOutcome.Failed(decodeWorkError(r));
            case 8 -> new ActivationOutcome.WorkflowDone(r.bool(), r.bytes());
            default -> throw new IllegalArgumentException(
                    "postcard: unknown ActivationOutcome discriminant " + discriminant);
        };
    }

    private static void encodeItemEffect(Postcard.Writer w, ItemEffect effect) {
        encodeVerdict(w, effect.verdict());
        w.vec(effect.changes(), Postcard.Writer::bytes);
        w.vec(effect.outputs(), ActivationCodec::encodeOutput);
        w.optionalVarint(effect.consumedOffset()).varint(effect.eventTime());
        w.vec(effect.timers(), TimerOp::encode);
        w.bool(effect.checkpoint());
        w.optional(effect.dlq(), DlqEntry::encode);
    }

    private static ItemEffect decodeItemEffect(Postcard.Reader r) {
        return new ItemEffect(
                decodeVerdict(r),
                r.vec(Postcard.Reader::bytes),
                r.vec(ActivationCodec::decodeOutput),
                r.optionalVarint(),
                r.varint(),
                r.vec(TimerOp::decode),
                r.bool(),
                r.some() ? Optional.of(DlqEntry.decode(r)) : Optional.empty());
    }

    private static void encodeVerdict(Postcard.Writer w, Verdict verdict) {
        switch (verdict) {
            case Verdict.Ok ignored -> w.varint(0);
            case Verdict.RetryableError error -> w.varint(1).string(error.message());
            case Verdict.NonRetryableError error -> w.varint(2).string(error.message());
        }
    }

    private static Verdict decodeVerdict(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        return switch (discriminant) {
            case 0 -> new Verdict.Ok();
            case 1 -> new Verdict.RetryableError(r.string());
            case 2 -> new Verdict.NonRetryableError(r.string());
            default -> throw new IllegalArgumentException("postcard: unknown Verdict discriminant " + discriminant);
        };
    }

    private static void encodeOutput(Postcard.Writer w, Output output) {
        switch (output.target()) {
            case OutputTarget.Edge edge -> w.varint(0)
                    .varint(Integer.toUnsignedLong(edge.channel()))
                    .varint(Integer.toUnsignedLong(edge.consumer()));
            case OutputTarget.Plugin plugin -> w.varint(1).string(plugin.channel());
        }
        w.optional(output.key(), Postcard.Writer::string).optionalVarint(output.eventTime());
        w.vec(output.metadata(), ActivationCodec::encodeMetadataPair);
        w.bytes(output.payload());
    }

    private static Output decodeOutput(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        OutputTarget target =
                switch (discriminant) {
                    case 0 -> new OutputTarget.Edge(Math.toIntExact(r.varint()), Math.toIntExact(r.varint()));
                    case 1 -> new OutputTarget.Plugin(r.string());
                    default -> throw new IllegalArgumentException(
                            "postcard: unknown OutputTarget discriminant " + discriminant);
                };
        return new Output(
                target,
                optionalString(r),
                r.optionalVarint(),
                r.vec(m -> Map.entry(m.string(), m.string())),
                r.bytes());
    }

    private static void encodePrepareResult(Postcard.Writer w, PrepareResult result) {
        switch (result) {
            case PrepareResult.Ok ok -> {
                w.varint(0).vec(ok.answer().keys(), Postcard.Writer::string);
                w.optionalVarint(ok.answer().eventTime());
            }
            case PrepareResult.Err err -> {
                w.varint(1);
                encodeWorkError(w, err.error());
            }
        }
    }

    private static PrepareResult decodePrepareResult(Postcard.Reader r) {
        int discriminant = Math.toIntExact(r.varint());
        return switch (discriminant) {
            case 0 -> new PrepareResult.Ok(new PrepareAnswer(r.vec(Postcard.Reader::string), r.optionalVarint()));
            case 1 -> new PrepareResult.Err(decodeWorkError(r));
            default -> throw new IllegalArgumentException(
                    "postcard: unknown PrepareResult discriminant " + discriminant);
        };
    }

    private static void encodeWorkError(Postcard.Writer w, WorkError error) {
        w.string(error.message()).bool(error.nonRetryable());
    }

    private static WorkError decodeWorkError(Postcard.Reader r) {
        return new WorkError(r.string(), r.bool());
    }

    // ------------------------------------------------------------------
    // Shared primitives
    // ------------------------------------------------------------------

    private static void encodeMetadataPair(Postcard.Writer w, Map.Entry<String, String> pair) {
        w.string(pair.getKey()).string(pair.getValue());
    }

    private static Optional<String> optionalString(Postcard.Reader r) {
        return r.some() ? Optional.of(r.string()) : Optional.empty();
    }

    private static TimerKind timerKind(Postcard.Reader r) {
        int ordinal = Math.toIntExact(r.varint());
        TimerKind[] kinds = TimerKind.values();
        if (ordinal < 0 || ordinal >= kinds.length) {
            throw new IllegalArgumentException("postcard: unknown TimerKind ordinal " + ordinal);
        }
        return kinds[ordinal];
    }
}
