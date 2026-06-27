package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.relay.Unit;
import java.util.List;

/**
 * Handler for {@code RemRelay}. Removes the {@link Unit} (relay or task)
 * from the registry; for a relay, pending mailbox items on its partitions
 * are dropped without being drained (matches Rust SDK v1 — bounded
 * mailboxes / drain-on-remove is tracked in a future change). This is also
 * the teardown the controller drives on a {@code TaskCompleted}.
 */
public final class RemRelayHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        String relayId = request.getRemRelay().getRelayId();
        Unit removed = ctx.relayInstanceRegistry().remove(relayId);
        if (removed == null) {
            errors.add("Relay not found");
        }
        return new byte[0];
    }
}
