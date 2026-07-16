package com.flezha.test;

import com.flezha.sdk.authoring.Journal;
import com.flezha.sdk.authoring.State;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/** Canonical keyed-collection reference state: a rolling per-key window. */
@State
public class RollingWindowState {
    @Journal(Journal.Mode.MAP_DEQUE)
    public Map<String, Deque<Double>> window = new HashMap<>();
}
