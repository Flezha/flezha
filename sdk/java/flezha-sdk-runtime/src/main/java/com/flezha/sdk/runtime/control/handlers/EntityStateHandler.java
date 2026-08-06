package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer.Selection;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer.Selector;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Handles the controller's {@code EntityState} command: serialize live
 * in-memory Entity state as JSON.
 *
 * <p>The command's optional {@code procedure} / {@code entity_key} selector
 * narrows the walk — absent selector covers every Procedure (the
 * migration/handover shape), {@code procedure} narrows to one Procedure's
 * Entities, and {@code procedure} + {@code entity_key} to one Entity, whose
 * {@code change_seq} is then stamped on the reply frame's metadata. A read
 * takes each Entity's state lock but does NOT drain pending Changes, so the
 * value it returns may be ahead of the WAL — which is what the seq is for.
 */
public final class EntityStateHandler implements CommandHandler {

    /**
     * Reply-frame metadata key carrying the {@code change_seq} a single
     * selected Entity was serialized at. Absent when the selector covers more
     * than one.
     */
    public static final String CHANGE_SEQ_META_KEY = "flz.change_seq";

    @Override
    @Nonnull
    public byte[] handle(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors)
            throws Exception {
        return reply(ctx, request, errors).rawData();
    }

    @Override
    @Nonnull
    public Reply reply(@Nonnull RunnerContext ctx, @Nonnull CommandRequest request, @Nonnull List<String> errors)
            throws Exception {
        String operativeId = request.getEntityState().getOperativeId();
        UnboundedOperativeInstance instance = ctx.operativeInstanceRegistry().getUnboundedOperative(operativeId);
        if (instance == null) {
            errors.add("UnboundedOperative with id " + operativeId + " not found");
            return new Reply(new byte[0], Map.of());
        }

        Selector selector = Selector.of(
                request.getEntityState().getProcedure(),
                request.getEntityState().getEntityKey());

        try {
            Selection selection = UnboundedOperativeStateSerializer.select(instance, selector);
            Map<String, String> metadata = selection.changeSeq().isPresent()
                    ? Map.of(
                            CHANGE_SEQ_META_KEY,
                            Long.toString(selection.changeSeq().getAsLong()))
                    : Map.of();
            return new Reply(selection.json(), metadata);
        } catch (UnboundedOperativeStateSerializer.RefusedException refused) {
            errors.add(refused.getMessage());
            return new Reply(new byte[0], Map.of());
        }
    }
}
