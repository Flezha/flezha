package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;

/** Single-procedure stateful authoring-sugar twin of {@link StoreInputProcedure}. */
@Procedure(id = "sugar_store_input", sink = "sugar_store_sink")
public class SugarStore {
    public String run(String input, SugarStoreStateHandle state) {
        state.pushTextInputs(input);
        return input;
    }
}
