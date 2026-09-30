# An HTTP gateway

So far only the CLI reached a session. On this page agentdesk gets an HTTP side: an
unbounded operator, `gateway`, carries the HTTP plugin that
[`custom_plugins.md`](custom_plugins.md) installed, and a browser follows a session's
answers as they are written, over server-sent events.

> #### Not a production setup {: .warning}
>
> The example runs with authorization off, and the HTTP plugin authenticates nobody: anyone
> who can reach the port can read any session's answers. The plugin page's
> [security section](../../plugins/http_server.md#security) says what the plugin guards and
> what it leaves to the operator.

## A request is an input

The plugin turns every HTTP request into one input of the operator it is enabled on, as a
JSON envelope: the method, the path, the body and a few more fields. Whatever a `response()`
end of the operator produces from that input is the HTTP answer. The plugin has no routes of
its own: the operator's graph routes on the method and the path.

The gateway answers `GET /sessions/<user>/events` with the user's session, named after the
user as Anna's and Bob's are, and any other request with 404.

> Write the request, the answer, and the tasklet that answers.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Request.java: one HTTP request, as the plugin hands it to the gateway.
@JsonIgnoreProperties(ignoreUnknown = true)                  // the envelope carries more than the gateway reads
public record Request(String method, String path, String body) {

    private static final Pattern ROUTE = Pattern.compile("/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)");

    boolean listens() {
        return is("GET", "events");
    }

    String user() {
        return route(1);
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

The pattern accepts only a short lower-case name as the user, because the user names a
session. `messages` is the route of [`operator_calls.md`](operator_calls.md).

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

An answer is the JSON object the plugin expects: a status and a body, or the session to
stream. Unset fields are left out, because the plugin refuses a key it does not know.

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

<!-- tabs-close -->

## The gateway

The gateway is one chain: each request, on the entry channel `http`, goes to `watch`, and
`watch`'s answer to the `response()` end. It runs for as long as the desk does, so it is an
unbounded operator.

> Write the gateway's operator, and register its factory in the runner.
> {: .step}

<!-- tabs-open -->

### Java

```java
// GatewayOperator.java: answers every request.
public final class GatewayOperator implements UnboundedOperator {

    @Override
    public String factoryId() {
        return "gateway";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(Request.class, "http")
                .into(Watch.class)
                .response();                                 // a stream, or 404
    }
}
```

In `Main`, after the `agent` factory:

```java
                .registerFactory(GatewayOperator.class)
```

<!-- tabs-close -->

> Build the runner again and replace its container.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
```

<!-- tabs-close -->

```sh
$ docker compose up -d --build --wait runner
```

## Start the gateway

Inside a container, the plugin must listen on every address, `0.0.0.0`: its default,
loopback, cannot be reached from outside the container. `compose.yaml` publishes the port on
the host's loopback only, so only this machine reaches it.

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

`--Xchannel=http` is the entry channel every request arrives on.
[`http_server.md`](../../plugins/http_server.md#options) lists the plugin's other options.

> Ask for a path the gateway does not know.
> {: .step}

```sh
$ curl -si localhost:8080/sessions/anna
```

```output
HTTP/1.1 404 Not Found
...
content-type: text/plain; charset=utf-8

no such route
```

## Stream an answer

When `watch` answers with a session to stream, `{"stream":"anna"}`, the plugin attaches a
listener to that session and keeps the response open: every part and every output on the
session's sink `answer` is written as one event, as it happens, until the client goes away.
The gateway is not in that path; it only named the session.

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

```output
{"text":"Message 6: you said \"Tell me more\". I am a demo model.","tokens":12}
```

The stream shows every token as the demo model wrote it, then the answer:

```output
:

event: part
data: {"attempt":1,"channel":"answer","part":0,"payload":"{\"text\":\"Message \",\"tokens\":1}"}

event: part
data: {"attempt":1,"channel":"answer","part":1,"payload":"{\"text\":\"6: \",\"tokens\":2}"}

...

event: output
data: {"channel":"answer","payload":"{\"text\":\"Message 6: you said \\\"Tell me more\\\". I am a demo model.\",\"tokens\":12}"}
```

The first line, `:`, is a comment the plugin writes when the stream opens, and again whenever
the stream has been quiet for a while, which keeps proxies from closing it. A failed turn is a
`dead_lettered` event, and the plugin page's [streams section](../../plugins/http_server.md#streams)
lists every event. A browser reads the same stream with `EventSource`, and that section has
the few lines that show a growing answer.

## Next

[`operator_calls.md`](operator_calls.md) lets a POST send a session a message, through a call
from the gateway.
