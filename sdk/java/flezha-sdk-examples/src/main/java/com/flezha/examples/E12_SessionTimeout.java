package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.TimerKind;
import com.flezha.sdk.state.WallClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * <b>12 — a session that closes itself when it goes quiet.</b>
 *
 * <p>Every other stream path in Flezha is input-driven. This one is not: the
 * whole reason a timer exists is the last statement of {@code run}. Without it,
 * nothing would ever close a session whose user simply stopped clicking —
 * there is no input to carry that decision, only the passage of time.
 *
 * <pre>{@code
 * ctx.timerAfter(IDLE);   // ...and in 30s, if nothing else happens, close me
 * }</pre>
 *
 * <p>Three things are worth reading closely:
 *
 * <ol>
 *   <li><b>The clock is a WALL clock, deliberately.</b> {@code timerAfter} is
 *       anchored on {@code ctx.now()}, and the deadline this Procedure wants is
 *       "30 seconds of real silence". An event-time timer would never fire here
 *       — nothing arriving is exactly the case it cannot see.
 *   <li><b>Each click retracts the deadline it supersedes.</b> Registration
 *       returns the absolute timestamp; that value is the handle the cancel
 *       takes. Both are durable ops on the pending set.
 *   <li><b>{@code firedAt} is the SCHEDULED instant</b>, not the wall clock at
 *       firing — so the record written below is identical on a replay.
 * </ol>
 *
 * <p>Nothing here is scheduler state. The pending set is folded out of this
 * Entity's own change log, so a crash-resume owes exactly the deadline it owed
 * before, and a firing's Change and the timer's removal are one durable record.
 *
 * @see <a href="../../../../../../../docs/sdk/entity_timers.md">docs/sdk/entity_timers.md</a>
 */
public final class E12_SessionTimeout {

    private E12_SessionTimeout() {}

    /** How long a session may stay quiet before it is closed. */
    private static final Duration IDLE = Duration.ofSeconds(30);

    /** One mutation of a session: a click, or the close its idle timer produced. */
    public static final class SessionChange implements Change {
        private String clicked = "";
        private long closedAt = -1L;

        public SessionChange() {}

        static SessionChange clicked(String page) {
            SessionChange c = new SessionChange();
            c.clicked = page;
            return c;
        }

        static SessionChange closed(long at) {
            SessionChange c = new SessionChange();
            c.closedAt = at;
            return c;
        }

        public String clicked() {
            return clicked;
        }

        public long closedAt() {
            return closedAt;
        }
    }

    /** One user's session. */
    public static final class SessionState {
        public final List<String> clicks = new ArrayList<>();
        public boolean closed;
    }

    /** What a closed session reports downstream. */
    public record ClosedSession(String user, int clicks, long closedAt) {}

    public static final class SessionWindow
            implements Procedure<String, SessionState, ClosedSession, SessionChange, WallClock, Void> {

        /**
         * The deadline currently pending, so each click can retract the last
         * one. Per-Entity scratch: the runtime holds one Procedure instance per
         * Entity.
         */
        private long deadline = -1L;

        @Override
        public String name() {
            return "session_window";
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(SessionChange.class, SessionState.class);
        }

        @Override
        public SessionState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new SessionState();
        }

        @Override
        public void applyChange(SessionState state, SessionChange change) {
            if (change.closedAt() >= 0) {
                state.closed = true;
            } else {
                state.clicks.add(change.clicked());
            }
        }

        @Override
        public void run(String click, StateHandle<SessionState, SessionChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            state.apply(SessionChange.clicked(click));

            if (deadline >= 0) {
                ctx.cancelProcessingTimeTimer(deadline);
            }
            deadline = ctx.timerAfter(IDLE);
        }

        @Override
        public void onTimer(
                long firedAt, TimerKind kind, StateHandle<SessionState, SessionChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            deadline = -1L;
            int clicks = state.get().clicks.size();
            state.apply(SessionChange.closed(firedAt));
            ctx.emit(new ClosedSession("", clicks, firedAt));
        }
    }

    public static final class SessionOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "session_window";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // One Entity per user: the key is declared on the WIRING, and each
            // Entity owns its own idle deadline.
            c.input(String.class, "clicks")
                    .keyBy(click -> click.split(":")[0])
                    .into(SessionWindow.class)
                    .named("closed_sessions")
                    .sink("kafka");
        }
    }
}
