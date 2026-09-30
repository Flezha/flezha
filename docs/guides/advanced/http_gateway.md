# An HTTP gateway

So far every message reached a session from the CLI. On this page agentdesk gets an HTTP
side: an unbounded operator, `gateway`, carries the HTTP plugin that
[`custom_plugins.md`](custom_plugins.md) installed, and a POST is a message for a user's
session. The gateway answers it with 202 at once and calls the session; the answer
itself reaches the browser on [`streaming_to_the_browser.md`](streaming_to_the_browser.md).

> #### Not a production setup {: .warning}
>
> The example runs with authorization off, and the HTTP plugin authenticates nobody:
> anyone who can reach the port can send any user's session a message and read its
> answers. The plugin page's [security section](../../plugins/http_server.md#security) says
> what the plugin guards and what it leaves to the operator.

## The gateway

The plugin turns every HTTP request into one input of the operator it is enabled on, on
an entry channel, as a JSON envelope: the method, the path, the body and a few more
fields. Whatever a `response()` end of the operator produces from that input is the
HTTP answer. The plugin has no routes of its own: the gateway's graph routes on the
method and the path.

The gateway knows two routes. `POST /sessions/<user>/messages` is a message for the
user's session, which is named after the user, as Anna's and Bob's are, and its body is
the message. `GET /sessions/<user>/events` is the session's answers, for the next page.

> Write the gateway's operator.
> {: .step}

<!-- tabs-open -->

### Java

```java
// GatewayOperator.java: answers every request, and calls the session a message is for.
public final class GatewayOperator implements UnboundedOperator {

    static final long DEADLINE_MS = 60_000;

    @Override
    public String factoryId() {
        return "gateway";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.effectivelyOnce();                                 // each request and answer on disk before it runs
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

### Rust

```rust
// src/gateway.rs: answers every request, and calls the session a message is for.
const DEADLINE_MS: u64 = 60_000;

pub struct GatewayOperator;

impl UnboundedOperator for GatewayOperator {
    fn factory_id() -> &'static str {
        "gateway"
    }

    fn channels(c: &ChannelBuilder) {
        c.effectively_once(); // each request and answer on disk before it runs
        let http = c.input::<Request>("http");
        http.filter(|request| !request.sends())
            .into::<Watch>()
            .response(); // a stream, or 404
        let verdicts = http
            .filter(Request::sends)
            .map(|request| Event::Sent(request.message()))
            .key_by(|event| event.user().to_string()) // one cell per user
            .into::<Budget>();
        verdicts.into::<Admit>().response(); // 202, or 429 over the budget
        verdicts
            .filter(Verdict::admitted)
            .map(|verdict| verdict.message)
            .key_by(|message| message.user.clone()) // the user's session
            .call::<Answer>(CallTarget::factory("agent").channel("message"), DEADLINE_MS)
            .map(Event::used)
            .key_by(|event| event.user().to_string())
            .into::<Budget>(); // the tokens the answer used
    }
}
```

<!-- tabs-close -->

A request that is not a message goes to `watch`, whose answer is a `response()` end. A
message goes to `budget`, one cell per user, which keeps each user's tokens for the day:
[`token_budget.md`](token_budget.md) is about it, and until a user has used the day's
tokens it admits every message. Its verdict goes two ways. `admit` answers the request,
through a second `response()` end: a graph may have several, and whichever answers
first answers the request. And an admitted message is a call to the user's session.
`effectivelyOnce()` is the delivery guarantee, which
[`effectively_once.md`](effectively_once.md) comes back to.

`call` ends a chain in a call to another operator, as
[`calls.md`](../../sdk/calls.md#operator-calls-call) describes. `CallTarget.factory("agent")`
after `keyBy` reaches the instance of `agent` whose id is the key, the user, and starts
one with the message as its first input when there is none, so a new user needs no
`flz operator start`. The call waits for nothing: its outcome comes back later as a
`Response` on the gateway's own Response channel, within `DEADLINE_MS` at the latest,
and `budget` reads the tokens from it.

> Write the request, the answer, and the two tasklets that answer.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Request.java: one HTTP request, as the plugin hands it to the gateway.
@JsonIgnoreProperties(ignoreUnknown = true)                  // the envelope carries more than the gateway reads
public record Request(String method, String path, String body) {

    private static final Pattern ROUTE = Pattern.compile("/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)");

    boolean sends() {
        return is("POST", "messages");
    }

    boolean listens() {
        return is("GET", "events");
    }

    String user() {
        return route(1);
    }

    Message message() {
        return new Message(user(), Objects.requireNonNullElse(body, ""));
    }

    private boolean is(String verb, String what) {
        return method.equals(verb) && route(2).equals(what);
    }

    private String route(int part) {                         // "" when the path is not a route
        Matcher route = ROUTE.matcher(path);
        return route.matches() ? route.group(part) : "";
    }
}
```

The pattern accepts only a short lower-case name as the user, because the user is the id
of the session the gateway calls. The body is the plugin's text of the request body,
absent when it is not valid UTF-8.

```java
// Http.java: the gateway's answer to a request.
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Http(Integer status, String body, String stream) {

    static Http of(int status, String body) {
        return new Http(status, body, null);
    }

    static Http stream(String session) {
        return new Http(null, null, session);
    }
}
```

An answer is the JSON object the plugin expects: a status and a body, sent as text, or
the session to stream. Unset fields are left out, because the plugin refuses a key it
does not know.

```java
// Watch.java: a session's events, or 404.
@Tasklet.Spec(name = "watch")
public final class Watch implements Tasklet<Request, Void, Http> {

    @Override
    public void run(Request request, Ctx<Void, Http> ctx) throws TaskletException {
        ctx.emit(request.listens() ? Http.stream(request.user()) : Http.of(404, "no such route"));
    }
}
```

```java
// Admit.java: 202 when the message goes on, 429 over the budget.
@Tasklet.Spec(name = "admit")
public final class Admit implements Tasklet<Verdict, Void, Http> {

    @Override
    public void run(Verdict verdict, Ctx<Void, Http> ctx) throws TaskletException {
        ctx.emit(verdict.admitted()
                ? Http.of(202, "sent to " + verdict.message().user())
                : Http.of(429, verdict.message().user() + " has used " + verdict.spent() + " of today's "
                        + Budget.DAILY_TOKENS + " tokens"));
    }
}
```

`Budget.java`, with the `Event`, `Sent`, `Used` and `Verdict` records the graph names, is
on [`token_budget.md`](token_budget.md#count-the-tokens); copy it from the example.

### Rust

```rust
// src/request.rs: one HTTP request, as the plugin hands it to the gateway.
#[derive(Clone, Deserialize)]
pub struct Request {
    method: String,
    path: String,
    body: Option<String>,
}

impl Request {
    pub fn sends(&self) -> bool {
        self.is("POST", "messages")
    }

    pub fn listens(&self) -> bool {
        self.is("GET", "events")
    }

    pub fn user(&self) -> &str {
        self.route().map_or("", |(user, _)| user)
    }

    pub fn message(&self) -> Message {
        Message {
            user: self.user().to_string(),
            text: self.body.clone().unwrap_or_default(),
        }
    }

    fn is(&self, verb: &str, what: &str) -> bool {
        self.method == verb && self.route().is_some_and(|(_, route)| route == what)
    }

    // `/sessions/<user>/<what>`, the user a short lower-case name; `None` when not a route.
    fn route(&self) -> Option<(&str, &str)> {
        let (user, what) = self.path.strip_prefix("/sessions/")?.split_once('/')?;
        let name = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-';
        let is_user = user.len() <= 64
            && user.starts_with(|c: char| c.is_ascii_lowercase())
            && user.chars().all(name);
        (is_user && matches!(what, "messages" | "events")).then_some((user, what))
    }
}
```

`route` accepts only a short lower-case name as the user, because the user is the id of
the session the gateway calls. The body is the plugin's text of the request body,
absent when it is not valid UTF-8. Fields the envelope carries beside these three are
ignored.

```rust
// src/http.rs: the gateway's answer to a request.
#[derive(Serialize, Deserialize)]
#[serde(untagged)] // the plugin reads the fields, with no name for the variant around them
pub enum Http {
    Plain { status: u16, body: String },
    Stream { stream: String },
}
```

An answer is the JSON object the plugin expects: a status and a body, sent as text, or
the session to stream. `#[serde(untagged)]` writes a variant as its fields alone,
because the plugin refuses a key it does not know.

```rust
// src/watch.rs: a session's events, or 404.
#[derive(Default)]
pub struct Watch;

#[tasklet(name = "watch")]
impl Tasklet for Watch {
    type Input = Request;
    type Output = Http;

    fn run(&mut self, request: Request, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(if request.listens() {
            Http::Stream {
                stream: request.user().to_string(),
            }
        } else {
            Http::Plain {
                status: 404,
                body: "no such route".to_string(),
            }
        })
    }
}
```

```rust
// src/admit.rs: 202 when the message goes on, 429 over the budget.
#[derive(Default)]
pub struct Admit;

#[tasklet(name = "admit")]
impl Tasklet for Admit {
    type Input = Verdict;
    type Output = Http;

    fn run(&mut self, verdict: Verdict, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let user = &verdict.message.user;
        ctx.emit(if verdict.admitted() {
            Http::Plain {
                status: 202,
                body: format!("sent to {user}"),
            }
        } else {
            Http::Plain {
                status: 429,
                body: format!(
                    "{user} has used {} of today's {DAILY_TOKENS} tokens",
                    verdict.spent
                ),
            }
        })
    }
}
```

`src/budget.rs`, with the `Event` and `Verdict` types the graph names, is on
[`token_budget.md`](token_budget.md#count-the-tokens); copy it from the example.

<!-- tabs-close -->

## A second way into a session

A call sends the value its chain carries, and `keyBy` reads the session's id from that
same value, so the gateway sends the user with the text. The session reads the text out
of it on an entry channel of its own, `message`, and `turn` stays as it was for the CLI.

> Add the message, give `agent` its second entry channel, and register the gateway's
> factory in the runner.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Message.java: a message for a user's session, as the gateway sends it.
public record Message(String user, String text) {}
```

In `AgentOperator.channels`, after the `turn` chain:

```java
        c.input(Message.class, "message")                // from the gateway
                .map(String.class, Message::text)
                .into(Reply.class);
```

`into(Reply.class)` names the tasklet the `turn` chain already feeds, so both channels
reach the same `reply`, with the same conversation. In `Main`:

```java
                .registerFactory(GatewayOperator.class)
```

### Rust

```rust
// src/agent.rs: a message for a user's session, as the gateway sends it.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub user: String,
    pub text: String,
}
```

In `AgentOperator::channels`, after the `turn` chain:

```rust
        c.input::<Message>("message") // from the gateway
            .map(|message| message.text)
            .into::<Reply>();
```

`into::<Reply>()` names the tasklet the `turn` chain already feeds, so both channels
reach the same `reply`, with the same conversation. In `main`, with `mod gateway;` among
the modules:

```rust
        .register_factory(gateway::GatewayOperator)?
```

<!-- tabs-close -->

## Start the gateway

Inside a container, the plugin must listen on every address, `0.0.0.0`: its default,
loopback, cannot be reached from outside the container. agentdesk's `compose.yaml`
publishes the port on the host's loopback only, `127.0.0.1:8080`
([`agent_session.md`](agent_session.md#the-containers)), so only this machine reaches it.

> Build the runner again and replace its container, from the project's folder.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

### Rust

```sh
$ cargo build --release
```

<!-- tabs-close -->

```sh
$ docker compose up -d --build --wait runner
```

> Start the gateway, and enable the HTTP plugin on it.
> {: .step}

```sh
$ flz operator start gateway --id gateway --namespace agents
```

```output
Started gateway
```

```sh
$ flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
```

```output
Enabling plugin...
Plugin web enabled successfully.
```

`--Xchannel=http` is the entry channel every request arrives on. The plugin's options,
such as how long a request waits for its answer, are listed on
[`http_server.md`](../../plugins/http_server.md#options).

## Send a message

> Send Anna a message over HTTP, from the host.
> {: .step}

```sh
$ curl -si -d 'Hello from the web' localhost:8080/sessions/anna/messages
```

```output
HTTP/1.1 202 Accepted
date: Wed, 30 Sep 2026 05:22:09 GMT
content-length: 12
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

sent to anna
```

The 202 comes from `admit` before the session has read the message: the gateway does
not wait for the answer.

> Send Carol a message. She has no session yet.
> {: .step}

```sh
$ curl -si -d 'Hi, I am Carol' localhost:8080/sessions/carol/messages
```

```output
HTTP/1.1 202 Accepted
date: Wed, 30 Sep 2026 05:22:09 GMT
content-length: 13
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

sent to carol
```

```sh
$ flz state get carol/reply --namespace agents
```

```output
state get carol/reply:
  change_seq: 1
  state:      {"reply":{"messages":["Hi, I am Carol"]}}
```

The call started Carol's session, and her first message is its first turn.

> Ask for a path the gateway does not know.
> {: .step}

```sh
$ curl -si localhost:8080/sessions/carol
```

```output
HTTP/1.1 404 Not Found
date: Wed, 30 Sep 2026 05:22:10 GMT
content-length: 13
vary: accept-encoding
cache-control: max-age=0, private, must-revalidate
content-type: text/plain; charset=utf-8

no such route
```

A request that fails in the plugin or in the call, such as one no answer came for in
time, is answered with the status the plugin page's
[table](../../plugins/http_server.md#status-of-every-outcome) gives it.

## Next

[`streaming_to_the_browser.md`](streaming_to_the_browser.md) sends the answer to the
browser as it is written.
