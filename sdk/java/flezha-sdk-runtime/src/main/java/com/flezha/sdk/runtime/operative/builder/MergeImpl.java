package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.Merge;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.operative.StreamOperativeConfigurationException;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.runtime.operative.EdgeYield;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

/**
 * A merged subscription under construction: the source channels declared so
 * far, each with the mapping that lifts its payload into {@code I}.
 */
final class MergeImpl<I> implements Merge<I> {

    /** One declared merge source: the channel and the chain ending in its mapping. */
    private record MergeSourceEntry(int channelId, int chain) {}

    private final StreamOperativeTopologyBuilder owner;
    private final Class<I> mergedType;
    private final List<MergeSourceEntry> sources = new ArrayList<>();
    /**
     * Every {@code from} the author wrote, including ones already refused.
     * The arity check reads this so a duplicated source is reported once,
     * as a duplicate — not also as a merge that declared too few.
     */
    private int declared;
    /**
     * The one key function a merge declares, applied to the merged value on
     * every source's edge.
     */
    private Function<Object, String> key;

    private boolean duplicateKey;

    MergeImpl(StreamOperativeTopologyBuilder owner, Class<I> mergedType) {
        this.owner = owner;
        this.mergedType = mergedType;
    }

    @Override
    @Nonnull
    @SuppressWarnings("unchecked")
    public <S> Merge<I> from(@Nonnull EdgeStream<S> source, @Nonnull Function<S, I> map) {
        owner.checkMutable();
        if (!(source instanceof EdgeSite site)) {
            throw new IllegalArgumentException("merge source was not declared on this operative's builder");
        }
        int channelId = site.siteChannelId();
        declared++;
        if (sources.stream().anyMatch(src -> src.channelId() == channelId)) {
            owner.error(StreamOperativeConfigurationException.duplicateMergeSource(owner.channel(channelId).name)
                    .getMessage());
            return this;
        }
        // The mapping composes AFTER any transform declared on the source
        // handle, which is what makes a source transform see that source's
        // own payload type — and what makes a key declared there trip the
        // transform-after-key refusal, i.e. "keying is declared once, on
        // the merge".
        int chain = owner.extendPending(
                channelId, site.sitePending(), mergedType, value -> EdgeYield.one(map.apply((S) value)), null);
        sources.add(new MergeSourceEntry(channelId, chain));
        return this;
    }

    @Override
    @Nonnull
    public Merge<I> keyBy(@Nonnull Function<I, String> key) {
        owner.checkMutable();
        if (this.key != null) {
            duplicateKey = true;
        }
        this.key = value -> key.apply(mergedType.cast(value));
        return this;
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(@Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure) {
        return wire(procedure, null);
    }

    @Override
    @Nonnull
    public <O> Stream<O> into(
            @Nonnull Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedure, @Nonnull String nodeName) {
        return wire(procedure, nodeName);
    }

    /**
     * Close the merge on its consumer, named or not — the one body behind
     * both {@code into} spellings, and the seam a packaged building block
     * ({@code routeLatest}) closes its own merge with instead of re-deriving
     * the named/unnamed dispatch.
     */
    <O> Stream<O> wire(Class<? extends Procedure<I, ?, O, ?, ?, ?>> procedureClass, String nodeName) {
        owner.checkMutable();
        // Register the consumer before its sources — as Rust's `Merge::wire`
        // does — so the arity refusal can name it even with no source to
        // wire it. `registerNode` resolves by identity, so wiring below
        // returns this same node.
        int node = owner.registerNode(procedureClass, nodeName);
        if (declared < 2) {
            owner.error(StreamOperativeConfigurationException.mergeTooFewSources(owner.procedure(node).name, declared)
                    .getMessage());
        }
        if (duplicateKey && !sources.isEmpty()) {
            owner.error(StreamOperativeConfigurationException.duplicateKeyBy(
                            owner.channel(sources.get(0).channelId()).name)
                    .getMessage());
        }
        for (MergeSourceEntry source : sources) {
            // The key rides each source's chain, so it is evaluated on the
            // mapped value.
            Integer chain = key == null
                    ? source.chain()
                    : owner.extendPending(source.channelId(), source.chain(), mergedType, null, key);
            owner.wireConsumer(source.channelId(), procedureClass, nodeName, mergedType, chain);
        }
        return new StreamImpl<>(owner, node);
    }
}
