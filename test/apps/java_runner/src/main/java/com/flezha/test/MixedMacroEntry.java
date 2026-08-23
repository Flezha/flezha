package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;

/** Sugared pass-through entry procedure of {@link MixedStreamOperative}. */
@Procedure(id = "mixed_macro_entry_procedure")
public class MixedMacroEntry {
    public String run(String input) {
        return input;
    }
}
