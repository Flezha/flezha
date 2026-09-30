# Streaming to the browser

A POST to the gateway is answered with 202 before the session has read the message, so
the answer has to reach the browser another way. On this page it streams there over
server-sent events, token by token, from the same parts and output that
`flz operator listen` showed on [`streamed_answers.md`](streamed_answers.md).

## A stream answer

`watch`, from [`http_gateway.md`](http_gateway.md#the-gateway), answers
`GET /sessions/<user>/events` with the user's session to stream, which the plugin receives as
`{"stream":"<user>"}`. That names an operator of the gateway's namespace, the user's
session. The plugin then attaches a listener to the session and keeps the response open:
every part and every output on the session's sink `answer` is written as one event, as
it happens, until the client goes away. The gateway is not in that path; it only named
the session.

## Watch an answer stream

> In a second terminal, open Anna's stream for five seconds.
> {: .step}

```sh
$ curl -sN --max-time 5 localhost:8080/sessions/anna/events
```

> In the first terminal, send Anna a message.
> {: .step}

```sh
$ curl -s -d 'Tell me more' localhost:8080/sessions/anna/messages
```

```output
sent to anna
```

The stream shows every token as the demo model wrote it, then the answer:

```output
:

event: part
data: {"attempt":1,"channel":"answer","part":0,"payload":"{\"text\":\"Message \",\"tokens\":1}"}

event: part
data: {"attempt":1,"channel":"answer","part":1,"payload":"{\"text\":\"7: \",\"tokens\":2}"}

event: part
data: {"attempt":1,"channel":"answer","part":2,"payload":"{\"text\":\"you \",\"tokens\":3}"}

event: part
data: {"attempt":1,"channel":"answer","part":3,"payload":"{\"text\":\"said \",\"tokens\":4}"}

event: part
data: {"attempt":1,"channel":"answer","part":4,"payload":"{\"text\":\"\\\"Tell \",\"tokens\":5}"}

event: part
data: {"attempt":1,"channel":"answer","part":5,"payload":"{\"text\":\"me \",\"tokens\":6}"}

event: part
data: {"attempt":1,"channel":"answer","part":6,"payload":"{\"text\":\"more\\\". \",\"tokens\":7}"}

event: part
data: {"attempt":1,"channel":"answer","part":7,"payload":"{\"text\":\"I \",\"tokens\":8}"}

event: part
data: {"attempt":1,"channel":"answer","part":8,"payload":"{\"text\":\"am \",\"tokens\":9}"}

event: part
data: {"attempt":1,"channel":"answer","part":9,"payload":"{\"text\":\"a \",\"tokens\":10}"}

event: part
data: {"attempt":1,"channel":"answer","part":10,"payload":"{\"text\":\"demo \",\"tokens\":11}"}

event: part
data: {"attempt":1,"channel":"answer","part":11,"payload":"{\"text\":\"model.\",\"tokens\":12}"}

event: output
data: {"channel":"answer","payload":"{\"text\":\"Message 7: you said \\\"Tell me more\\\". I am a demo model.\",\"tokens\":12}"}
```

The first line, `:`, is a comment the plugin writes when the stream opens; it writes one
again whenever the stream has been quiet for a while, which keeps proxies from closing
it. Each `part` event is one token, with its number and attempt: a turn that is tried
again starts its parts at 0 again. The `output` event is the finished answer on the
sink. A failed turn is a `dead_lettered` event instead, and the plugin page's
[streams section](../../plugins/http_server.md#streams) lists every event.

## In a browser

A browser reads the same stream with `EventSource`, from a page the same origin serves,
and the plugin page's [streams section](../../plugins/http_server.md#streams) has the few
lines that show a growing answer. `EventSource` reconnects on its own when a stream
breaks, and starts again at the live edge: tokens written while it was away are not
sent again, but the `output` event of a later answer carries the whole of it.

This gateway streams whichever session the path names, to anyone who reaches the port.
A gateway in front of real users checks that the user asking is the user named, by a
cookie or a query parameter, since `EventSource` sends no custom header.

## Next

[`token_budget.md`](token_budget.md) gives each user a daily token budget.
