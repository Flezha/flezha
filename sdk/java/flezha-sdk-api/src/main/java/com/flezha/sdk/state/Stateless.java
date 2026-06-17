package com.flezha.sdk.state;

/**
 * Empty per-partition state for stateless sugared shards. The annotation
 * processor uses this as the {@code State} type when a {@code @Shard} class's
 * {@code run} method declares no state parameter, so a generated stateless
 * shard still satisfies the {@link com.flezha.sdk.shard.Shard} type surface
 * with a no-op {@code initState}.
 */
public final class Stateless {

    public Stateless() {}

    @Override
    public boolean equals(Object o) {
        return o instanceof Stateless;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "Stateless";
    }
}
