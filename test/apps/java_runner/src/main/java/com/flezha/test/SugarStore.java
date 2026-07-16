package com.flezha.test;

import com.flezha.sdk.authoring.Operation;

/** Single-operation stateful authoring-sugar twin of {@link StoreInputOperation}. */
@Operation(id = "java_sugar_store", sink = "java_sugar_store_sink")
public class SugarStore {
    public String run(String input, SugarStoreStateHandle state) {
        state.pushTextInputs(input);
        return input;
    }
}
