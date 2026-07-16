package com.flezha.test;

import com.flezha.sdk.authoring.Journal;
import com.flezha.sdk.authoring.State;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Per-mode coverage for {@code MAP_SET} + {@code MAP_VEC}, plus a flat field. */
@State
public class KeyedCollectionsState {
    @Journal(Journal.Mode.MAP_SET)
    public Map<String, Double> last = new HashMap<>();

    @Journal(Journal.Mode.MAP_VEC)
    public Map<String, List<Double>> closes = new HashMap<>();

    public long ticks;
}
