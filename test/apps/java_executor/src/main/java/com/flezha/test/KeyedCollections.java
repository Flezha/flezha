package com.flezha.test;

import com.flezha.sdk.authoring.Shard;
import java.util.List;

/** Exercises the generated {@code MAP_SET} / {@code MAP_VEC} mutators. */
@Shard(id = "java_keyed_collections")
public class KeyedCollections {
    public void run(WindowInput input, KeyedCollectionsStateHandle state) {
        state.setLast(input.key(), input.value());
        state.pushCloses(input.key(), input.value());
        List<Double> closes = state.get().closes.get(input.key());
        if (closes.size() > 3) {
            state.drainClosesFront(input.key(), closes.size() - 3);
        }
        state.setTicks(state.get().ticks + 1);
    }
}
