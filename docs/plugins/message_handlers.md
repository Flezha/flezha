# Operative message handlers

For plugin authors and controller-side callers who need to ask a running
operative a question and get a reply without touching its IO streams. The
`:on_message` extension point routes a fixed BEAM envelope to the one handler
plugin registered under a key on the target's Overwatch; the first-party
`MessageForward` provider bounces the request to the SDK over the
`OperativeMessage` control-channel command. This doc covers attaching a
handler, the request envelope, timeouts, the provider callback,
`MessageForward`, the wire command and the state of the SDK-side hook, and the
caveats.

To **feed** an operative — asynchronous, durable data flow into its input
stream — use [`operative_forward.md`](operative_forward.md) instead. The two
are complementary: different destinations, different durability.

## Attaching a handler

A handler is an ordinary `%Flezha.Plugin{}` subscribed to `:on_message` with a
`key`. Enable it at runtime with `Flezha.Service.PluginManager.enable_plugin/2`
or attach it at start through `--plugins`:

```elixir
alias Flezha.Plugin
alias Flezha.Plugin.Provider.MessageForward
alias Flezha.Service.PluginManager

PluginManager.enable_plugin(
  "orders-fetch-init",
  Plugin.new(
    id: "orders-fetch-init",
    namespace: "default",
    operative_id: "orders-aggregator",
    provider: MessageForward,
    opts: %{},
    subscriptions: [{:on_message, [key: :fetch_init_state]}]
  )
)
```

```bash
flz operative start orders-aggregator order_aggregator --plugins \
  '[{"id": "orders-fetch-init", "provider": "operative_message_forward",
     "subscriptions": [["on_message", {"key": "fetch_init_state"}]]}]'
```

| Rule | Result |
|---|---|
| `key` missing | `{:error, {:invalid_subscription_opts, :missing_key}}` |
| `key` already registered on that operative | `{:error, {:key_conflict, :on_message, key}}`; the existing handler is left untouched |
| `position` given | `{:error, :position_not_applicable}` — a request has one subscriber, not a chain |

Conflicts surface at enable time, not at dispatch time. When `operative_id`
names a partition group, `enable_plugin/2` fans out once per member, injecting
that member's partition opts and suffixing the plugin id with `#pK`; a partial
enable reports an error naming each failed member. A group id has no single
Overwatch — the envelope below is per member.

## Request envelope

Callers send a fixed envelope directly to the target operative's Overwatch pid
(`Flezha.Service.OperativeManager.get_operative_pid/2`):

```elixir
ref = make_ref()
send(overwatch_pid, {:flezha_request, handler_key, payload, self(), ref})

receive do
  {:flezha_response, ^ref, {:ok, reply_bytes}} -> ...
  {:flezha_response, ^ref, {:error, reason}} -> ...
after
  timeout_ms -> {:error, :timeout}
end
```

`payload` is opaque `binary()`. The Overwatch looks the handler up by key and
runs `provider.on_message/2` in a Task under
`Flezha.Operative.MessageHandlerTaskSup`; an exception becomes
`{:error, {:exception, kind, reason}}`, and the Task sends
`{:flezha_response, ref, result}` to the caller. The Overwatch is never
blocked on a handler — IO and lifecycle messages are served while it runs.

| Condition | Reply |
|---|---|
| no handler under `handler_key` (or no `:on_message` plugin at all) | `{:flezha_response, ref, {:error, {:unknown_key, :on_message, key}}}` |
| malformed envelope | none — the Overwatch logs and drops the message, and the caller's `after` clause fires |

The plugin installer waits up to 5 s for in-flight handler Tasks before it
swaps a plugin's code.

## Timeouts

The caller owns the deadline. There is no server-side deadline propagation: a
late reply lands in the caller's mailbox and is discarded by selective
`receive` (the ref no longer matches). A caller that is itself running under a
budget — an init-state provider under its `timeout_ms`, say — keeps its request
timeout strictly below that budget so it answers cleanly instead of being
killed.

## Provider callback

A provider exports `on_message/2`, receiving the request context and the
plugin's `opts`:

```elixir
defmodule MyApp.CachedReply do
  alias Flezha.Plugin.Extension.Ctx

  def on_message(%Ctx.Msg{} = ctx, _opts) do
    case MyApp.Cache.get(ctx.operative_id) do
      nil -> {:error, :cache_miss}
      value -> {:ok, value}
    end
  end
end
```

`%Flezha.Plugin.Extension.Ctx.Msg{}` carries:

| Field | Type | Meaning |
|---|---|---|
| `key` | `atom()` | the matched handler key |
| `payload` | `binary()` | the request bytes |
| `reply_to` | `pid()` | the caller — the only handle for authorization gating |
| `ref` | `reference()` | the reply correlation |
| `operative_id` | `String.t()` | target operative's id |
| `namespace` | `atom() \| String.t()` | target operative's namespace |
| `runner_id` | `String.t() \| nil` | the runner hosting the target |

Return `{:ok, binary()}` for success or `{:error, term()}` for a structured
error.

## First-party `MessageForward` provider

`Flezha.Plugin.Provider.MessageForward` (provider id
`operative_message_forward`) wraps
`Flezha.Runner.ControlUnit.send_operative_message/3`: it sends an
`OperativeMessage` on the runner's control channel and awaits the
`OperativeMessageResponse` under the control unit's 5 s reply timeout. An
empty `error` field maps to `{:ok, payload}`; a non-empty one to
`{:error, error}`. Transport failures surface as `{:error, :timeout}`,
`{:error, {:control_unit_exit, reason}}` or `{:error, {:decode_error, reason}}`.

### Control-channel command

```proto
message OperativeMessage {
    string operative_id = 1;
    bytes payload = 2;
}

message OperativeMessageResponse {
    bytes payload = 1;
    string error = 2;  // empty on success
}
```

`OperativeMessage` is field 10 of the `CommandRequest.command` oneof and rides
the existing control stream with the same primitives as `EntityState` — no new
stream class. The SDK's reply is the protobuf-encoded
`OperativeMessageResponse` in the outer `CommandResponse.raw_data`; see
[`../sdk/protocol_spec.md`](../sdk/protocol_spec.md).

### SDK-side state

No SDK implements a per-Procedure `handle_message` hook yet. The shared kernel
answers **every** `OperativeMessage` with
`OperativeMessageResponse{ error: "operative does not support handle_message" }`
(`flz_core/src/command/operative_message.rs`), so `MessageForward` returns
`{:error, "operative does not support handle_message"}` for any Rust or Java
operative and the operative stays healthy. The hook is planned; the round-trip
tests that exercise it are parked until it lands. Until then a `MessageForward`
handler proves the routing, and any real reply comes from an Elixir-side
`on_message/2` provider.

## Caveats and scope

- **Open authorization.** Any process that can reach the Overwatch pid can
  send a request; a handler's own code may inspect `reply_to` to gate requests.
  Namespace-scoped ACLs are not offered.
- **Cross-node latency eats the caller's budget.** A request/reply across BEAM
  nodes consumes the caller's `timeout_ms`; size it for the distribution round
  trip, and keep it under any enclosing budget.
- **In flight during a handover.** Handler registrations travel with the
  operative's plugins. A request that resolves the Overwatch before the
  handover and sends after the old pid is gone times out; one sent during the
  registry gap finds no pid; one sent after the swap is answered by the
  re-registered handler. None of these crash the caller or either Overwatch.
- **Control-channel contention.** A large `OperativeMessage.payload` shares
  the control stream with every other command. Keep payloads small.
- **Not offered.** Fire-and-forget, publish/subscribe, streaming replies,
  fan-out to several handlers per key, rate limiting, deadline propagation to
  the SDK.

## See also

- [`operative_forward.md`](operative_forward.md) — feeding an operative's input stream instead
- [`init_state_providers.md`](init_state_providers.md) — a caller that runs under a start-time budget
- [`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) — the wire command
- [`lifecycle.md`](lifecycle.md) — plugin enable/disable mechanics
