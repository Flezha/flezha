# HTTP API plugin

The HTTP API plugin lets AI tools and scripts operate a controller from another process or
machine. It serves the controller's published operations two ways: as MCP tools at
`POST /mcp`, for Claude Code and other MCP clients, and as plain JSON under `/v1`. Every
call carries a `flz` token and is made as that token's principal, with exactly what its role
allows. `FlezhaHttp.ApiProvider` is its provider, under the provider id `http_api`, in the
`flezha_http` package (`plugins/flezha_http_plugin`).

| | |
| --- | --- |
| Type | controller plugin, enabled with no hook |
| Ships | package `flezha_http` |
| Namespace | `_system` only; any other is refused (`plugin.namespace_refused`) |
| Runs | one listener on every controller that holds `_system` |
| MCP | revision `2026-07-28`, and `2025-11-25` for clients that open with `initialize`; tools only, no session |

**Contents**

1. [Enable it](#enable-it)
2. [Options](#options)
3. [A token for an assistant](#a-token-for-an-assistant)
4. [Connect Claude Code](#connect-claude-code)
5. [Plain JSON](#plain-json)
6. [What is never exposed](#what-is-never-exposed)
7. [Limits and gotchas](#limits-and-gotchas)
8. [See also](#see-also)

## Enable it

Install the package first ([installing_plugins.md](../topics/plugins/installing_plugins.md)).
Then, with an admin token:

```sh
$ flz plugin enable api http_api --namespace _system
$ flz plugin list --namespace _system
```

```output
Enabling plugin...
Plugin api enabled.
api  http_api  bound: none  runs on: flezha@127.0.0.1
```

It listens on `127.0.0.1:8765`. The plugin is recorded like any other, so it starts again
with the controller. `flz plugin disable api --namespace _system` stops every listener.

To listen on another address, give it a certificate:

```sh
$ flz plugin enable api http_api --namespace _system \
  --Xbind=0.0.0.0 \
  --Xtls_certfile=/etc/flezha/tls/cert.pem \
  --Xtls_keyfile=/etc/flezha/tls/key.pem
```

Without one it is refused (`http.tls_required`), because a token sent in the clear can be
replayed. Set `plain_http` only when TLS ends in front of it, at a proxy on the same host.

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `port` | int | `8765` | no | The TCP port the API listens on, 1 to 65535 |
| `bind` | string | `"127.0.0.1"` | no | The address it listens on: an IPv4 or IPv6 literal. Any but loopback needs TLS or `plain_http` |
| `tls_certfile` | string | none | no | The PEM certificate it serves HTTPS with; given with `tls_keyfile` |
| `tls_keyfile` | string | none | no | The PEM private key of `tls_certfile` |
| `plain_http` | bool | `false` | no | Serve plain HTTP on an address that is not loopback, where TLS ends in front of it |
| `allowed_origins` | names | `[]` | no | The browser origins a request may come from; a request naming any other is answered 403 |
| `deadline_ms` | int | `30000` | no | How long a call waits for the controller's answer, 100 to 600000 |
| `max_body_bytes` | int | `1048576` | no | The largest request body accepted, up to 16777216; a larger one is answered 413 |
| `max_waiting` | int | `64` | no | Calls waiting for an answer at once, up to 10000; one more is answered 503 |
| `max_connections` | int | `128` | no | Connections open at once, up to 10000; one more is closed at once |
<!-- provider-options:end -->

## A token for an assistant

Give an assistant its own token, in one namespace, with the narrowest role that does its
job: `viewer` to look, `ops` to start, send and handle dead letters. Never give it an
`admin@_system` token. The roles and what each allows are on
[authorization.md](../topics/basics/authorization.md#roles).

```sh
$ flz token issue assistant default ops
```

```output
token issued for assistant (ops@default)
id:    cd5d04f277fb
token (shown once, store it now): flzt_mceqxmydtjgzolxgmgow7ffybum5ztfb5vrasr5ab2djqaansnja
```

The assistant sees only the tools its role allows. A call the role does not allow is
refused with an `authz.*` code, and the refusal is audited like any other.

## Connect Claude Code

```sh
$ export FLEZHA_TOKEN=flzt_mceqxmydtjgzolxgmgow7ffybum5ztfb5vrasr5ab2djqaansnja
$ claude mcp add --transport http flezha http://127.0.0.1:8765/mcp --header "Authorization: Bearer $FLEZHA_TOKEN"
$ claude mcp list
```

```output
flezha: http://127.0.0.1:8765/mcp (HTTP) - ✔ Connected
```

Each tool is one operation, named after it in snake case: `operator_start`,
`operator_send`, `dlq_list`, `dlq_replay`. A failed call comes back as a tool error
carrying the error object, so the model reads its `code` and `hint` and can correct
itself. Any other MCP client connects the same way: the URL and the header.

## Plain JSON

The same operations without MCP. `POST /v1/call/<Name>` takes the arguments as a JSON
array, in the order the catalogue gives them, and answers what `flz ... --json` prints:

```sh
$ curl -s -X POST http://127.0.0.1:8765/v1/call/Operator.list \
  -H "Authorization: Bearer $FLEZHA_TOKEN" -d '["default"]'
```

```output
[{"factory":"echo","health":"healthy","id":"my_echo","node":"flezha@127.0.0.1","partition":null,"runner":"default_151bd2853a67"}]
```

`GET /v1/catalogue` lists the operations the token may call, with their arguments. A
failure answers the error object with the HTTP status of its class: `404` for
`dlq.not_found`, `403` for `authz.role_insufficient`.

## What is never exposed

Whatever the token: stopping the controller, issuing or revoking tokens, binding or
unbinding roles, deleting a namespace and claiming the admin are not reachable over HTTP,
nor is any operation that streams (`flz operator listen`, `flz observe tail`). They are
not listed, and a call naming one is answered as an unknown operation. Use `flz` on the
controller's machine for those ([cli.md](../topics/cli/cli.md)).

## Limits and gotchas

- **A token works on the controller that issued it.** Each controller checks tokens
  against its own store, so in a cluster a client must reach the controller its token
  came from.
- **One port per host.** Every controller holding `_system` binds the same port, so two
  controllers on one machine cannot both listen. One fails with `http.listen_failed`.
- **A port taken at start stays down.** If the port is in use when the listener starts,
  `flz plugin list` shows `runs on: idle`. Free the port, then disable and enable the
  plugin.
- **Dead letters of a namespace are listed by operator.** Listing every operator's is a
  `_system` read, so a namespace token passes the operator: `dlq_list` with
  `operator_class`.
- **A call past `deadline_ms` answers `admin.timeout`.** The operation itself carries on.
- **Tool lists are kept for a minute.** A client may keep the tool list it read for 60
  seconds, so a role changed since shows within the minute. Every call is still judged
  by the role the token holds now.
- **Connections are capped before the token is read.** `max_connections` bounds open
  connections, idle ones included, so put the listener where only its clients reach it.
- **Nothing secret is logged.** The listener logs the method, the path, the status, the
  duration and the tool, at debug level, never a header, a body or a token.

## See also

- [authorization.md](../topics/basics/authorization.md): tokens, roles and the audit trail
- [hooks.md](../topics/plugins/hooks.md): controller plugins
- [http_server.md](http_server.md): an operator behind HTTP, in the same package
