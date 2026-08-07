package com.flezha.test;

import java.util.ArrayList;
import java.util.List;

/** The session's inputs and the timestamps its idle timer closed it at. */
public final class SessionTimeoutState {
    public final List<String> touched = new ArrayList<>();
    public final List<Long> closedAt = new ArrayList<>();
}
