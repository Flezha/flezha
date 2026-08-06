package com.flezha.test;

import com.flezha.sdk.authoring.Procedure;
import com.flezha.sdk.authoring.EntitiesFor;
import java.util.List;

/** Static-entity entry procedure of the 2-procedure reference operative. */
@Procedure(id = "sugar_static_entry_procedure", keys = {"a", "b", "c", "d"})
public class SugarEntry {

    @EntitiesFor
    public List<String> entitiesFor(String input) {
        char c = input.isEmpty() ? ' ' : input.charAt(0);
        return switch (c) {
            case 'a' -> List.of("a");
            case 'b' -> List.of("b");
            case 'c' -> List.of("c");
            default -> List.of("d");
        };
    }

    public String run(String input, SugarEntryStateHandle state) {
        state.pushSeen(input);
        return input;
    }
}
