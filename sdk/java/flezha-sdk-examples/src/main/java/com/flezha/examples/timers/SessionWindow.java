package com.flezha.examples.timers;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/** One user's session, closed once it has been quiet for a while. */
public final class SessionWindow
        implements Procedure<String, SessionWindow.SessionState, ClosedSession, SessionChange, WallClock, Void> {

    /** How long a session may stay quiet before it is closed. */
    private static final Duration IDLE = Duration.ofSeconds(30);

    public static final class SessionState {
        public String user = "";
        public final List<String> clicks = new ArrayList<>();
        public boolean closed;
    }

    /**
     * The deadline currently pending, so each click can retract the last one. A plain field is
     * fine here: the runtime holds one procedure instance per entity, and a restart re-arms.
     */
    private long deadline = -1L;

    @Override
    @Nonnull
    public String name() {
        return "session_window";
    }

    @Override
    @Nonnull
    public List<Class<?>> kryoTypes() {
        return List.of(SessionChange.class, SessionState.class, ArrayList.class);
    }

    @Override
    @Nonnull
    public SessionState initState(
            @Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        SessionState state = new SessionState();
        state.user = entityKey.orElse("");
        return state;
    }

    @Override
    public void applyChange(@Nonnull SessionState state, @Nonnull SessionChange change) {
        if (change.closedAt() >= 0) {
            state.closed = true;
        } else {
            state.clicks.add(change.clicked());
        }
    }

    @Override
    public void run(
            @Nonnull String click, @Nonnull StateHandle<SessionState, SessionChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(SessionChange.clicked(click));

        // The pattern: retract the deadline you set last time, then set a new one. Without the
        // cancel you would accumulate one live timer per click and close the session early.
        if (deadline >= 0) {
            ctx.cancelProcessingTimeTimer(deadline);
        }
        deadline = ctx.timerAfter(IDLE);
    }

    @Override
    public void onTimer(
            long firedAt,
            @Nonnull TimerKind kind,
            @Nonnull StateHandle<SessionState, SessionChange> state,
            @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        deadline = -1L;
        SessionState session = state.get();
        int clicks = session.clicks.size();
        state.apply(SessionChange.closed(firedAt));
        ctx.emit(new ClosedSession(session.user, clicks, firedAt));
    }
}
