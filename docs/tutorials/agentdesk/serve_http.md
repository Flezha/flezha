# Serve HTTP

On this page you start the gateway, an operator that runs in flow behind the
HTTP plugin, and stream Anna's answer to an HTTP client. Each request is an
input on the channel `http`, and what the graph sends to its `response()` end is
the answer. [The HTTP server plugin](../../plugins/http_server.md) covers its
options.

<!-- tabs-open -->

### Java

```java
// GatewayOperator.java: requests in, answers out
public final class GatewayOperator implements Operator {
    static final long DEADLINE_MS = 60_000;

    @Override
    public String factoryId() {
        return "gateway";
    }

    @Override
    public Run run() {
        return new Run.Flow(new Exactly.EffectivelyOnce());
    }

    @Override
    public void channels(ChannelBuilder c) {
        EntryChannel<Request> http = c.input(Request.class, "http");
        http.filter(request -> !request.sends())
                .into(Watch.class)
                .response();                                 // a stream, or 404
        Stream<Verdict> verdicts = http.filter(Request::sends)
                .map(Event.class, request -> new Sent(request.message()))
                .keyBy(Event::user)                          // one cell per user
                .into(Budget.class);
        verdicts.into(Admit.class)
                .response();                                 // 202, or 429 over the budget
        verdicts.filter(Verdict::admitted)
                .map(Message.class, Verdict::message)
                .keyBy(Message::user)                        // the user's session
                .call(CallTarget.factory("agent").channel("message"), Answer.class, DEADLINE_MS)
                .map(Event.class, Used::of)
                .keyBy(Event::user)
                .into(Budget.class);                         // the tokens the answer used
    }
}
```

[`GatewayOperator.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/GatewayOperator.java)

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
# lib/agentdesk/gateway_operator.ex: requests in, answers out
defmodule Agentdesk.GatewayOperator do
  use Flezha.SDK.Operator,
    factory_id: "gateway",
    run: {:flow, exactly: :effectively_once}

  @deadline_ms 60_000

  @impl true
  def channels(c) do
    http = input_with(c, "http", &Request.decode/1)

    http
    |> filter(&(not Request.sends?(&1)))
    |> into(Watch)
    |> response()

    verdicts =
      http
      |> filter(&Request.sends?/1)
      |> map(&Request.message/1)
      |> key_by(& &1.user)
      |> into(Budget)

    verdicts
    |> into(Admit)
    |> response()

    verdicts
    |> filter(&Budget.admitted?/1)
    |> map(& &1.message)
    |> key_by(& &1.user)
    |> call(
      factory: "agent",
      channel: "message",
      deadline_ms: @deadline_ms,
      decode: &Jason.decode/1
    )
    |> map(&Budget.used/1)
    |> key_by(& &1.user)
    |> into(Budget)
  end
end
```

[`lib/agentdesk/gateway_operator.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/agentdesk/lib/agentdesk/gateway_operator.ex)

<!-- tabs-close -->

The first route answers a request for a session's events with that session's
stream. The other routes are the next page's.

> Start the gateway, and enable the HTTP plugin on it.
> {: .step}

```sh
$ flz operator start gateway --id gateway --namespace agents
$ flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
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
$ flz operator call anna --namespace agents --text 'Tell me more'
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
