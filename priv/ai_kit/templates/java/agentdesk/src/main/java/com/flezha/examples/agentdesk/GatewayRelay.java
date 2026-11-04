package com.flezha.examples.agentdesk;

import com.flezha.examples.agentdesk.Budget.Event;
import com.flezha.examples.agentdesk.Budget.Sent;
import com.flezha.examples.agentdesk.Budget.Used;
import com.flezha.examples.agentdesk.Budget.Verdict;
import com.flezha.examples.agentdesk.Reply.Answer;
import com.flezha.sdk.channel.CallTarget;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.channel.EntryChannel;
import com.flezha.sdk.channel.Stream;
import com.flezha.sdk.relay.Relay;

/**
 * The HTTP side of the desk: a flow relay that carries the HTTP plugin, answers every
 * request, and calls the session a message is for.
 */
public final class GatewayRelay implements Relay {

    static final long DEADLINE_MS = 60_000;

    @Override
    public String factoryId() {
        return "gateway";
    }

    @Override
    public Run run() {
        // each request and answer on disk before it runs
        return new Run.Flow(new Exactly.EffectivelyOnce());
    }

    @Override
    public void channels(ChannelBuilder c) {
        // every request arrives on "http"; kept as a handle to split it
        EntryChannel<Request> http = c.input(Request.class, "http");
        // route 1: GET .../events gets the session's stream
        http.filter(request -> !request.sends())
                .into(Watch.class)
                .keyed()
                .output();                                   // a stream, or 404
        // route 2: POST .../messages is checked against the user's budget
        Stream<Verdict> verdicts = http.filter(Request::sends)
                .map(Event.class, request -> new Sent(request.message()))
                .keyBy(Event::user)                          // one cell per user
                .into(Budget.class);
        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        verdicts.into(Admit.class)
                .keyed()
                .output();                                   // 202, or 429 over the budget
        // use 2: pass an admitted message on to the user's session
        verdicts.filter(Verdict::admitted)
                .map(Message.class, Verdict::message)
                .keyBy(Message::user)                        // the user's session
                // call the "agent" relay; its answer comes back here
                .call(CallTarget.factory("agent").channel("message"), Answer.class, DEADLINE_MS)
                .map(Event.class, Used::of)                  // the session's answer, as tokens used
                .keyBy(Event::user)
                .into(Budget.class);                         // back into the same budget
    }
}
