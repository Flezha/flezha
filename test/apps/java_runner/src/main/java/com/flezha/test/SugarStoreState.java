package com.flezha.test;

import com.flezha.sdk.authoring.State;
import java.util.ArrayList;
import java.util.List;

/** Derives {@code SugarStoreStateChange} (set/push variants + apply + kryoTypes). */
@State
public class SugarStoreState {
    public List<String> text_inputs = new ArrayList<>();
}
