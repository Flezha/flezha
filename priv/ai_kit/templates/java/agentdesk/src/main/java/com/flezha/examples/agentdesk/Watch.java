package com.flezha.examples.agentdesk;

import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/** Answers a request for a session's events with its stream, and any other request with 404. */
@Tasklet.Spec(name = "watch")
public final class Watch implements Tasklet<Request, Void, Http> {

    @Override
    public void run(Request request, Ctx<Void, Http> ctx) throws TaskletException {
        // keyed by the user: an output of an effectively-once relay needs a key
        ctx.emitKeyed(
                request.user(), request.listens() ? Http.stream(request.user()) : Http.of(404, "no such route"));
    }
}
