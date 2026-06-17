package com.flezha.test;

import com.flezha.sdk.authoring.Shard;

/** Single-shard stateful authoring-sugar twin of {@link StoreInputShard}. */
@Shard(id = "java_sugar_store", sink = "java_sugar_store_sink")
public class SugarStore {
    public String run(String input, SugarStoreStateHandle state) {
        state.pushTextInputs(input);
        return input;
    }
}
