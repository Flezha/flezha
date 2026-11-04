package com.flezha.examples.agentdesk;

import com.flezha.examples.agentdesk.Reply.Answer;
import com.flezha.sdk.channel.Response;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Keeps the tokens a user's answers used today, and admits a message while they are under the
 * day's budget. One cell per user.
 */
@Tasklet.Spec(name = "budget")
public final class Budget implements Tasklet<Budget.Event, Budget.Spent, Budget.Verdict> {

    static final int DAILY_TOKENS = 40;

    public sealed interface Event {                          // what reaches a user's cell
        String user();
    }

    public record Sent(Message message) implements Event {  // a message to admit
        @Override
        public String user() {
            return message.user();
        }
    }

    public record Used(String user, int tokens) implements Event {    // the tokens an answer used
        static Used of(Response<Message, Answer> response) {          // a failed turn used none
            return new Used(response.request().user(), response.ok() ? response.answer().tokens() : 0);
        }
    }

    public record Verdict(Message message, int spent) {
        boolean admitted() {
            return spent < DAILY_TOKENS;
        }
    }

    public static class Spent {                              // what each user's cell remembers
        public String day = "";
        public int tokens;

        int on(String today) {
            return today.equals(day) ? tokens : 0;
        }
    }

    public record Counted(String day, int tokens) implements Change<Spent> {
        @Override
        public void applyTo(Spent spent) {
            spent.tokens = spent.on(day) + tokens;
            spent.day = day;
        }
    }

    @Override
    public void run(Event event, Ctx<Spent, Verdict> ctx) throws TaskletException {
        String today = LocalDate.ofInstant(Instant.ofEpochMilli(ctx.now()), ZoneOffset.UTC).toString();
        switch (event) {
            // A message to admit: hand on its verdict.
            case Sent sent -> ctx.emit(new Verdict(sent.message(), ctx.state().on(today)));
            // The tokens an answer used: count them.
            case Used used -> ctx.change(new Counted(today, used.tokens()));
        }
    }
}
