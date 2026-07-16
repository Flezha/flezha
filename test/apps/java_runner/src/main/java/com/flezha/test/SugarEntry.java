package com.flezha.test;

import com.flezha.sdk.authoring.Operation;
import com.flezha.sdk.authoring.SegmentFor;
import java.util.List;

/** Static-segmented entry operation of the 2-operation reference operative. */
@Operation(id = "sugar_static_entry_operation", keys = {"a", "b", "c", "d"})
public class SugarEntry {

    @SegmentFor
    public List<String> segmentFor(String input) {
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
