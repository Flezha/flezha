package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Mirror of the Rust runner's {@code SessionTimeoutProcedure}: each input pushes
 * the idle deadline out, and the firing closes the session.
 *
 * <p>A <b>processing-time</b> timer, because the answer to "nothing has arrived"
 * cannot be an event-time one — nothing would advance the clock. The timeout is
 * read from {@code TIMER_TEST_TIMEOUT_MS} so the e2e can set it short.
 */
public final class SessionTimeoutProcedure
        implements Procedure<String, SessionTimeoutState, byte[], SessionTimeoutChange, WallClock, Void> {

    @Override
    public String name() {
        return "session_timeout_procedure";
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(SessionTimeoutChange.class, SessionTimeoutState.class);
    }

    @Override
    public SessionTimeoutState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new SessionTimeoutState();
    }

    @Override
    public void applyChange(SessionTimeoutState state, SessionTimeoutChange change) {
        if (change.closedAt() >= 0) {
            state.closedAt.add(change.closedAt());
        } else {
            state.touched.add(change.touched());
        }
    }

    @Override
    public void run(String input, StateHandle<SessionTimeoutState, SessionTimeoutChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(SessionTimeoutChange.touched(input));
        ctx.timerAfter(Duration.ofMillis(timeoutMs()));
    }

    @Override
    public void onTimer(
            long firedAt,
            TimerKind kind,
            StateHandle<SessionTimeoutState, SessionTimeoutChange> state,
            ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(SessionTimeoutChange.closed(firedAt));
        ctx.emit(("closed@" + firedAt).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static long timeoutMs() {
        return TestEnv.longVar("TIMER_TEST_TIMEOUT_MS", 1_000L);
    }
}
