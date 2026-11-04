package com.flezha.examples.agentdesk;

import com.flezha.examples.agentdesk.Budget.Verdict;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/** Answers a message 202 when it goes on to the session, and 429 when its user is over the budget. */
@Tasklet.Spec(name = "admit")
public final class Admit implements Tasklet<Verdict, Void, Http> {

    @Override
    public void run(Verdict verdict, Ctx<Void, Http> ctx) throws TaskletException {
        ctx.emitKeyed(verdict.message().user(), verdict.admitted()
                ? Http.of(202, "sent to " + verdict.message().user())
                : Http.of(429, verdict.message().user() + " has used " + verdict.spent() + " of today's "
                        + Budget.DAILY_TOKENS + " tokens"));
    }
}
