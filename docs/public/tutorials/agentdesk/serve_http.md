# Serve HTTP

On this page you start the gateway, a relay that runs in flow behind the
HTTP plugin, and stream Anna's answer to an HTTP client. Each request is an
input on the channel `http`, and the first output the graph sends to an output is
the answer. [The HTTP server plugin](../http_server.html) covers its
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
```

[`GatewayRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/src/main/java/com/flezha/examples/agentdesk/GatewayRelay.java)

### Rust

```rust
// src/gateway.rs: requests in, answers out
const DEADLINE_MS: u64 = 60_000;

pub struct GatewayRelay;

impl Relay for GatewayRelay {
    fn factory_id() -> &'static str {
        "gateway"
    }

    fn channels(c: &ChannelBuilder) {
        // every request arrives on "http"; kept as a handle to split it
        let http = c.input::<Request>("http");
        // route 1: GET .../events gets the session's stream
        http.filter(|request| !request.sends())
            .into::<Watch>()
            .keyed()
            .output(); // a stream, or 404

        // route 2: POST .../messages is checked against the user's budget
        let verdicts = http
            .filter(Request::sends)
            .map(|request| Event::Sent(request.message()))
            .key_by(|event| event.user().to_string()) // one cell per user
            .into::<Budget>();

        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        verdicts.into::<Admit>().keyed().output(); // 202, or 429 over the budget

        // use 2: pass an admitted message on to the user's session
        verdicts
            .filter(Verdict::admitted)
            .map(|verdict| verdict.message)
            .key_by(|message| message.user.clone()) // the user's session
            // call the "agent" relay; its answer comes back here
            .call::<Answer>(CallTarget::factory("agent").channel("message"), DEADLINE_MS)
            .map(Event::used) // the session's answer, as tokens used
            .key_by(|event| event.user().to_string())
            .into::<Budget>(); // back into the same budget
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            // each request and answer on disk before it runs
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}
```

[`src/gateway.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/agentdesk/src/gateway.rs)

### Node

```javascript
// gateway_relay.mjs: requests in, answers out
const DEADLINE_MS = 60_000;
const user = (value) => value.user;

export default defineRelay({
  factoryId: 'gateway',
  flow: { exactly: 'effectively_once' }, // each request and answer on disk before it runs
  channels: (c) => {
    // every request arrives on "http"; kept as a handle to split it
    const http = c.input('http', Request.decode);
    // route 1: GET .../events gets the session's stream
    http
      .filter((request) => !request.sends())
      .into(Watch)
      .keyed()
      .output(); // a stream, or 404
    // route 2: POST .../messages is checked against the user's budget
    const verdicts = http
      .filter((request) => request.sends())
      .map((request) => request.message())
      .keyBy(user) // one cell per user
      .into(Budget);
    // a verdict: the message plus its user's tokens today; used twice
    // use 1: answer the POST
    verdicts.into(Admit).keyed().output(); // 202, or 429 over the budget
    // use 2: pass an admitted message on to the user's session
    verdicts
      .filter(admitted)
      .map((verdict) => verdict.message)
      .keyBy(user) // the user's session
      // call the "agent" relay; its answer comes back here
      .call({ factory: 'agent', channel: 'message' }, { deadlineMs: DEADLINE_MS })
      .map(used) // the session's answer, as tokens used
      .keyBy(user)
      .into(Budget); // back into the same budget
  },
});
```

[`gateway_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/agentdesk/gateway_relay.mjs)

### Python

```python
# gateway.py: requests in, answers out
DEADLINE_MS = 60_000


# each request and answer on disk before it runs
@flezha.relay("gateway", run={"flow": {"exactly": "effectively_once"}})
def gateway(c: flezha.ChannelBuilder) -> None:
    user = itemgetter("user")
    # every request arrives on "http"; kept as a handle to split it
    http = c.input("http").map(Request.of)
    # route 1: GET .../events gets the session's stream
    http.filter(lambda request: not request.sends()).into(
        Watch
    ).keyed().output()  # a stream, or 404
    # route 2: POST .../messages is checked against the user's budget
    verdicts = (
        http.filter(Request.sends)
        .map(sent)
        .key_by(user)  # one cell per user
        .into(Budget)
    )
    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts.into(Admit).keyed().output()  # 202, or 429 over the budget
    # use 2: pass an admitted message on to the user's session
    (
        verdicts.filter(admitted)
        .map(itemgetter("message"))
        .key_by(user)  # the user's session
        # call the "agent" relay; its answer comes back here
        .call(factory="agent", channel="message", deadline_ms=DEADLINE_MS)
        .map(used)  # the session's answer, as tokens used
        .key_by(user)
        .into(Budget)  # back into the same budget
    )
```

[`gateway.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/agentdesk/gateway.py)

### Ruby

```ruby
# gateway_relay.rb: requests in, answers out
class GatewayRelay < Flezha::Relay
  factory_id "gateway"
  run flow: { exactly: :effectively_once } # each request and answer on disk before it runs

  DEADLINE_MS = 60_000

  channels do |c|
    user = ->(value) { value[:user] }
    # every request arrives on "http"; kept as a handle to split it
    http = c.input("http", decode: Request.method(:decode))
    # route 1: GET .../events gets the session's stream
    http.filter { |request| !request.sends? }
        .into(Watch)
        .keyed
        .output # a stream, or 404
    # route 2: POST .../messages is checked against the user's budget
    verdicts = http.filter(&:sends?)
                   .map(&:message)
                   .key_by(&user) # one cell per user
                   .into(Budget)
    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts.into(Admit)
            .keyed
            .output # 202, or 429 over the budget
    # use 2: pass an admitted message on to the user's session
    verdicts.filter { |verdict| Budget.admitted?(verdict) }
            .map { |verdict| verdict[:message] }
            .key_by(&user) # the user's session
            # call the "agent" relay; its answer comes back here
            .call(factory: "agent", channel: "message", deadline_ms: DEADLINE_MS)
            .map { |response| Budget.used(response) } # the session's answer, as tokens used
            .key_by(&user)
            .into(Budget) # back into the same budget
  end
end
```

[`gateway_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/agentdesk/gateway_relay.rb)

### PHP

```php
// src/GatewayRelay.php: requests in, answers out
#[Relay(
    factoryId: 'gateway',
    run: ['flow' => ['exactly' => 'effectively_once']], // each request and answer on disk before it runs
)]
final class GatewayRelay extends \Flezha\Relay
{
    private const DEADLINE_MS = 60_000;

    public static function channels(ChannelBuilder $c): void
    {
        $user = fn (array $value) => $value['user'];
        // every request arrives on "http"; kept as a handle to split it
        $http = $c->input('http', Request::decode(...));
        // route 1: GET .../events gets the session's stream
        $http->filter(fn (Request $request) => !$request->sends())
            ->into(Watch::class)
            ->keyed()
            ->output();                              // a stream, or 404
        // route 2: POST .../messages is checked against the user's budget
        $verdicts = $http->filter(fn (Request $request) => $request->sends())
            ->map(fn (Request $request) => $request->message())
            ->keyBy($user)                           // one cell per user
            ->into(Budget::class);
        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        $verdicts->into(Admit::class)
            ->keyed()
            ->output();                              // 202, or 429 over the budget
        // use 2: pass an admitted message on to the user's session
        $verdicts->filter(Budget::admitted(...))
            ->map(fn (array $verdict) => $verdict['message'])
            ->keyBy($user)                           // the user's session
            // call the "agent" relay; its answer comes back here
            ->call(factory: 'agent', channel: 'message', deadlineMs: self::DEADLINE_MS)
            ->map(Budget::used(...))                 // the session's answer, as tokens used
            ->keyBy($user)
            ->into(Budget::class);                   // back into the same budget
    }
}
```

[`src/GatewayRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/agentdesk/src/GatewayRelay.php)

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
    |> output()

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
    |> output()

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

### Go

```go
// gateway.go: requests in, answers out
const deadlineMs = 60_000

type GatewayRelay struct{}

func (GatewayRelay) Spec() flezha.RelaySpec {
	return flezha.RelaySpec{
		FactoryID: "gateway",
		// each request and answer on disk before it runs
		Run: map[string]any{"flow": map[string]any{"exactly": "effectively_once"}},
	}
}

func (GatewayRelay) Channels(c *flezha.ChannelBuilder) {
	byUser := func(event Event) string { return event.User }

	// every request arrives on "http"; kept as a handle to split it
	http := flezha.Input[Request](c, "http")
	// route 1: GET .../events gets the session's stream
	listens := http.Filter(func(request Request) bool { return !request.Sends() })
	flezha.Into(listens, Watch{}).
		Keyed().
		Output("") // a stream, or 404

	// route 2: POST .../messages is checked against the user's budget
	sends := flezha.Map(http.Filter(Request.Sends), Sent).
		KeyBy(byUser) // one cell per user
	verdicts := flezha.Into(sends, Budget{})

	// a verdict: the message plus its user's tokens today; used twice
	// use 1: answer the POST
	flezha.Into(verdicts, Admit{}).
		Keyed().
		Output("") // 202, or 429 over the budget

	// use 2: pass an admitted message on to the user's session
	admitted := flezha.Map(verdicts.Filter(Verdict.Admitted), func(v Verdict) Message { return v.Message }).
		KeyBy(func(message Message) string { return message.User }) // the user's session
	// call the "agent" relay; its answer comes back here
	answers := flezha.Call[Message, Answer](admitted, flezha.CallSpec{
		Factory: "agent", Channel: "message", DeadlineMs: deadlineMs,
	})
	// the session's answer, as tokens used
	used := flezha.Map(answers, Used).KeyBy(byUser)
	flezha.Into(used, Budget{}) // back into the same budget
}
```

[`gateway.go`](https://github.com/flezha/flezha_go_sdk/blob/main/examples/agentdesk/gateway.go)

<!-- tabs-close -->

The first route answers a request for a session's events with that session's
stream. The other routes are the next page's. The gateway runs effectively once,
so each output is `keyed()`, and `Watch` and `Admit` key what they emit by the
user.

> Start the gateway, and enable the HTTP plugin on it.
> {: .step}

```sh
$ flz relay start gateway --id gateway --namespace agents
$ flz plugin enable web http_server --relay_id=gateway --namespace agents --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
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
