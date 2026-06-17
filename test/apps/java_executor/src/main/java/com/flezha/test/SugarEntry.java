package com.flezha.test;

import com.flezha.sdk.authoring.PartitionFor;
import com.flezha.sdk.authoring.Shard;
import java.util.List;

/** Static-partitioned entry shard of the 2-shard reference relay. */
@Shard(id = "sugar_static_entry_shard", keys = {"a", "b", "c", "d"})
public class SugarEntry {

    @PartitionFor
    public List<String> partition(String input) {
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
