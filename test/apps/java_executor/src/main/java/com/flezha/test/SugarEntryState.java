package com.flezha.test;

import com.flezha.sdk.authoring.State;
import java.util.ArrayList;
import java.util.List;

/** Entry-shard state for the 2-shard reference relay. */
@State
public class SugarEntryState {
    public List<String> seen = new ArrayList<>();
}
