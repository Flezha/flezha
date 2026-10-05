# Serve HTTP

On this page you start the gateway, a relay that runs in flow behind the
HTTP plugin, and stream Anna's answer to an HTTP client. Each request is an
input on the channel `http`, and the first output the graph sends to a sink is
the answer. [The HTTP server plugin](../../plugins/http_server.md) covers its
options.

<!-- tabs-open -->

### Java

```java
// GatewayRelay.java: requests in, answers out
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
                .sink();                                     // a stream, or 404
        // route 2: POST .../messages is checked against the user's budget
        Stream<Verdict> verdicts = http.filter(Request::sends)
                .map(Event.class, request -> new Sent(request.message()))
                .keyBy(Event::user)                          // one cell per user
                .into(Budget.class);
        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        verdicts.into(Admit.class)
                .keyed()
                .sink();                                     // 202, or 429 over the budget
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
```

[`GatewayRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/GatewayRelay.java)

### Rust

TODO: this page is not yet written for Rust.

### Node

TODO: this page is not yet written for Node.

### Python

TODO: this page is not yet written for Python.

### Ruby

TODO: this page is not yet written for Ruby.

### PHP

TODO: this page is not yet written for PHP.

### Elixir

```elixir
# lib/agentdesk/gateway_relay.ex: requests in, answers out
defmodule Agentdesk.GatewayRelay do
  use Flezha.SDK.Relay,
    factory_id: "gateway",
    # each request and answer on disk before it runs
    run: {:flow, exactly: :effectively_once}

  @deadline_ms 60_000

  @impl true
  def channels(c) do
    # every request arrives on "http"; kept as a handle to split it
    http = input_with(c, "http", &Request.decode/1)

    # route 1: GET .../events gets the session's stream
    http
    |> filter(&(not Request.sends?(&1)))
    |> into(Watch)
    |> keyed()
    # a stream, or 404
    |> sink()

    # route 2: POST .../messages is checked against the user's budget
    verdicts =
      http
      |> filter(&Request.sends?/1)
      |> map(&Request.message/1)
      # one cell per user
      |> key_by(& &1.user)
      |> into(Budget)

    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts
    |> into(Admit)
    |> keyed()
    # 202, or 429 over the budget
    |> sink()

    # use 2: pass an admitted message on to the user's session
    verdicts
    |> filter(&Budget.admitted?/1)
    |> map(& &1.message)
    # the user's session
    |> key_by(& &1.user)
    # call the "agent" relay; its answer comes back here
    |> call(
      factory: "agent",
      channel: "message",
      deadline_ms: @deadline_ms,
      decode: &Jason.decode/1
    )
    # the session's answer, as tokens used
    |> map(&Budget.used/1)
    |> key_by(& &1.user)
    # back into the same budget
    |> into(Budget)
  end
end
```

[`lib/agentdesk/gateway_relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/gateway_relay.ex)

<!-- tabs-close -->

The first route answers a request for a session's events with that session's
stream. The other routes are the next page's. The gateway runs effectively once,
so each sink is `keyed()`, and `Watch` and `Admit` key what they emit by the
user.

> Start the gateway, and enable the HTTP plugin on it.
> {: .step}

```sh
$ flz relay start gateway --id gateway --namespace agents
$ flz plugin enable web http_server --relay_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
```

```output
Started gateway
Enabling plugin...
Plugin web enabled.
Plugin web bound to gateway.
```

> In a second terminal, open Anna's stream for five seconds.
> {: .step}

```sh
$ curl -sN --max-time 5 localhost:8080/sessions/anna/events
```

> In the first terminal, send Anna a message.
> {: .step}

```sh
$ flz relay call anna --namespace agents --text 'Tell me more'
```

The stream shows each part, then the answer, as server-sent events:

```output
event: part
data: {"attempt":1,"channel":"answer","part":0,"payload":"{\"text\":\"Message \",\"tokens\":1}"}

...

event: output
data: {"channel":"answer","payload":"{\"text\":\"Message 6: you said \\\"Tell me more\\\". I am a demo model.\",\"tokens\":12}"}
```

## Next

[Call the session](call_the_session.md) sends messages over HTTP.
