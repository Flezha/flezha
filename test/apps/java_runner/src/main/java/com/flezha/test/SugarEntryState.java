package com.flezha.test;

import com.flezha.sdk.authoring.State;
import java.util.ArrayList;
import java.util.List;

/** Entry-procedure state for the 2-procedure reference operative. */
@State
public class SugarEntryState {
    public List<String> seen = new ArrayList<>();
}
