package com.flezha.test;

import com.flezha.sdk.authoring.State;
import java.util.ArrayList;
import java.util.List;

/** Entry-operation state for the 2-operation reference operative. */
@State
public class SugarEntryState {
    public List<String> seen = new ArrayList<>();
}
