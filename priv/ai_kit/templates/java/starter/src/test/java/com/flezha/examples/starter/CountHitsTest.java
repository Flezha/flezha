package com.flezha.examples.starter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.flezha.examples.starter.CountHits.Count;
import com.flezha.examples.starter.CountHits.Hit;
import com.flezha.examples.starter.CountHits.Seen;
import com.flezha.sdk.testing.TaskletTestHarness;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Runs the tasklet with no controller: one harness per page stands in for that page's cell. */
class CountHitsTest {

    private final Map<String, TaskletTestHarness<Hit, Seen, Count>> cells = new HashMap<>();

    @Test
    void countsEachPageOnItsOwn() {
        List<Long> totals = new ArrayList<>();
        for (String page : List.of("home", "home", "about", "home")) {
            totals.add(hit(page).total());
        }

        assertEquals(List.of(1L, 2L, 1L, 3L), totals);
    }

    @Test
    void theCountSurvivesARestart() {
        hit("home");
        hit("home");

        // Replaying the recorded changes rebuilds the state the cell had, as a restart does.
        assertTrue(cells.get("home").replayEquals().isEmpty());
    }

    private Count hit(String page) {
        TaskletTestHarness<Hit, Seen, Count> cell =
                cells.computeIfAbsent(page, key -> TaskletTestHarness.of(new CountHits()).withCellKey(key));
        cell.applyInputs(List.of(new Hit(page)));
        List<Object> emits = cell.recordedEmits();
        return (Count) emits.get(emits.size() - 1);
    }
}
