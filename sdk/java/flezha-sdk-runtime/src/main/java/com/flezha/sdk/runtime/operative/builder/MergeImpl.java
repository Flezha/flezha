package com.flezha.sdk.runtime.operative.builder;

import com.flezha.sdk.channel.EdgeStream;
import com.flezha.sdk.channel.Merge;
import com.flezha.sdk.channel.Stream;
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
     * The one key function a merge declares, applied to the merged value on
     * every source's edge. Declaring it twice records two `key` links on each
     * source's chain, which the core answers as DuplicateKeyBy.
     */
    private Function<Object, String> key;

    /** How many times {@code keyBy} was declared; a second is one link more. */
    private int keyDeclarations;

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
        // A repeat of one source is RECORDED, not refused here: the core reads
        // the group's sources off the record and names the duplicate.
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
        keyDeclarations++;
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
        // does — so a merge that declared none still records a procedure the
        // core can name. `registerNode` resolves by identity, so wiring below
        // returns this same node.
        int node = owner.registerNode(procedureClass, nodeName);
        int group = owner.nextMergeGroup();
        for (MergeSourceEntry source : sources) {
            // The key rides each source's chain, so it is evaluated on the
            // mapped value; a second keyBy records a second `key` link.
            Integer chain = source.chain();
            for (int i = 0; i < keyDeclarations; i++) {
                chain = owner.extendPending(source.channelId(), chain, mergedType, null, key);
            }
            owner.wireConsumer(source.channelId(), procedureClass, nodeName, mergedType, chain, group);
        }
        return new StreamImpl<>(owner, node);
    }
}
