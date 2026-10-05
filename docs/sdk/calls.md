# Calls: implementation notes

For SDK implementers and contributors. How to use it is on
[Relay calls](../topics/advanced/relay_calls.md).

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
