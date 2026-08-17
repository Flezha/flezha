package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.RoutedDocument;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The single-entity procedure {@link ChannelBuilder#routeLatest} expands to:
 * it retains the latest document, and hands it to a key on that key's own next
 * traffic item — never by fanning out over a remembered key set, because a
 * remembered set outlives key-group ownership (see the builder method for the
 * whole rationale).
 *
 * <p>Not part of the authoring surface. Rust keeps the equivalent type crate-private;
 * Java has no such visibility, and the runtime instantiates a Procedure
 * reflectively from a public class, so this one is public and says so instead.
 *
 * <p>The document is held as an {@code Object} — the block is generic over the
 * document type and erasure leaves nothing else to hold it as. Two consequences
 * worth knowing: the document's own class is registered with the state codec by
 * the wiring (which is what the {@code documentType} token is for), and a
 * <i>hot-swap migration</i> — the one path that moves state as JSON rather than
 * as the Kryo snapshot + change log — hands the retained document back as a
 * generic map unless the document type is itself a JSON scalar. A crash-resume,
 * a compaction and a rebind all restore through Kryo and are unaffected.
 */
public final class LatestDocumentRouter
        implements Procedure<
                LatestDocumentRouter.Input,
                LatestDocumentRouter.RouterState,
                RoutedDocument<Object>,
                LatestDocumentRouter.RouterChange,
                WallClock,
                Void> {

    /** The node identity, and so the derived channel name, of every router. */
    public static final String NODE_NAME = "route_latest";

    /**
     * The router's merged input: a document off the document stream, or the
     * Entity key one traffic item proved this member owns. Java has no sum
     * types, so this is the repo's one-populated-slot record with a static
     * factory per variant — and the factories are the merge mappings.
     *
     * <p>The traffic payload itself never arrives: the key function runs as an
     * edge transform, so the router holds no application values.
     */
    public record Input(@Nonnull Optional<Object> document, @Nonnull Optional<String> trafficKey) {
        @Nonnull
        public static Input ofDocument(@Nonnull Object document) {
            return new Input(Optional.of(document), Optional.empty());
        }

        @Nonnull
        public static Input ofTrafficKey(@Nonnull String key) {
            return new Input(Optional.empty(), Optional.of(key));
        }
    }

    /**
     * The router's journaled state: the retained document, the version that
     * identifies it, and the version each key has already received.
     *
     * <p>{@code delivered} grows with the keys this member sees, which is the
     * Entity count it already pays for — not a new unbounded-growth class.
     */
    public static final class RouterState {
        @Nullable
        public Object document;

        public long version;

        @Nonnull
        public Map<String, Long> delivered = new HashMap<>();
    }

    /**
     * One journaled mutation. {@code retain} present = the document was
     * replaced and the version advances with it (which is what makes every key
     * owed a delivery again); {@code deliveredKey} present = that key received
     * {@code version}.
     */
    public record RouterChange(@Nonnull Optional<Object> retain, @Nonnull Optional<String> deliveredKey, long version)
            implements Change {
        @Nonnull
        static RouterChange retain(@Nonnull Object document) {
            return new RouterChange(Optional.of(document), Optional.empty(), 0L);
        }

        @Nonnull
        static RouterChange delivered(@Nonnull String key, long version) {
            return new RouterChange(Optional.empty(), Optional.of(key), version);
        }
    }

    @Override
    @Nonnull
    public String name() {
        return NODE_NAME;
    }

    @Override
    @Nonnull
    public Class<Input> inputType() {
        return Input.class;
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public Class<RoutedDocument<Object>> outputType() {
        return (Class<RoutedDocument<Object>>) (Class<?>) RoutedDocument.class;
    }

    @Override
    @Nonnull
    public Class<RouterChange> changeType() {
        return RouterChange.class;
    }

    @Override
    @Nonnull
    public Class<WallClock> clockType() {
        return WallClock.class;
    }

    @Override
    @Nonnull
    public Class<Void> configType() {
        return Void.class;
    }

    /**
     * The block's own types. The <b>document</b> type is not here — only the
     * wiring knows it — and is appended to this node's registration list by
     * {@link ChannelBuilder#routeLatest}.
     */
    @Override
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return List.of(Input.class, RouterState.class, RouterChange.class);
    }

    @Override
    @Nonnull
    public RouterState initState(
            @Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new RouterState();
    }

    @Override
    public void applyChange(@Nonnull RouterState state, @Nonnull RouterChange change) {
        if (change.retain().isPresent()) {
            state.document = change.retain().get();
            state.version += 1;
            return;
        }
        state.delivered.put(change.deliveredKey().orElseThrow(), change.version());
    }

    @Override
    public void run(
            @Nonnull Input input, @Nonnull StateHandle<RouterState, RouterChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        RouterState current = state.get();
        if (input.document().isPresent()) {
            Object document = input.document().get();
            // Version 0 means "nothing retained", so the first document lands at
            // 1 and every key's absent entry is already behind it.
            if (!Objects.equals(current.document, document)) {
                state.apply(RouterChange.retain(document));
            }
            return;
        }
        String key = input.trafficKey().orElseThrow();
        Object document = current.document;
        if (document == null) {
            return;
        }
        long version = current.version;
        Long seen = current.delivered.get(key);
        if (seen != null && seen >= version) {
            return;
        }
        // Emit first: a failed emit rides the channel's retry ladder, and
        // recording the delivery before it would make the retry silent.
        ctx.emit(new RoutedDocument<>(key, document));
        state.apply(RouterChange.delivered(key, version));
    }
}
