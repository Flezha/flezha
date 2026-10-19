# Calls: implementation notes

For SDK implementers and contributors. How to use it is on
[Relay calls](../../public/topics/core/sending_and_calling.md#calls).

## The stamps

The runner core stamps `flz.reply_to` on every output whose input carried a call's token, and
on no other output; how that token ties an input to its answer is
[How a call finds its answer](../../public/topics/core/sending_and_calling.md#how-a-call-finds-its-answer).
A call end's request carries the target
(`flz.call`), the Response channel (`flz.call_response`), the deadline (`flz.deadline_at`), the
callee's entry channel (`flz.call_channel`) and answer channel (`flz.call_answer_channel`) when
the end names them, `flz.call_create` when it may not create its callee, and its key. A host
cannot write any of these keys: the core drops them from whatever a host put on an output.

The controller hands an output carrying a token to the caller waiting on it when the call names
no answer channel or names the output's own, removes the token, and passes the output on to the
`on_output` chain like any other. A relay caller's answer channel rides in the relay token
itself, so a callee that restarted since it took the call still answers from that channel.

## The `Response`

The Response channel is an entry channel named after the calling channel with `.response`
(`gateway.response`). Each `Response` carries the request as the call end wrote it, the result
(the answer, or the error that ended the call in the SDK's one error type, a timeout being
`call.timeout`), `target` (the instance that answered, a generated one included) and `created`
(whether this call started it), so a gateway tells a new session from one that expired.

| SDK | Type |
| --- | --- |
| Rust | `flezha_runner::Response<Req, Ans>`: `request`, `result: Result<Ans, FlezhaError>`, `target`, `created` |
| Java | `Response<Req, Ans>`: `request()`, `answer()`, `error()`, `ok()`, `target()`, `created()` |
| Node | `Response<Req, Ans>`: `request`, `value` or `error`, `target`, `created` |
| Python | `Response`: `request`, `value` or `error`, `target`, `created` |
| Ruby | `Flezha::Response`: `request`, `value` or `error`, `ok?`, `target`, `created` |
| PHP | `Flezha\Response`: `request`, `value` or `error`, `ok()`, `target`, `created` |
| Elixir | `%Flezha.SDK.Response{request, result: {:ok, value} \| {:error, error}, target, created}` |

On the wire a `Response` is one JSON document the controller writes, the request and the answer
in base64: `{"call", "target", "created", "request", "value" | "error"}`, `error` being the
error model's own document. `test/fixtures/call_response_envelope.json` holds the cases every
SDK decodes identically.
