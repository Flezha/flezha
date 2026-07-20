# Operative Message Handlers

Message-handler plugins let one operative ask another operative a typed
question and receive a typed reply without polluting the IO streams. The
Elixir controller's Overwatch routes a fixed envelope to a per-operative handler
plugin, which typically bounces the request into the SDK-side operative
factory via a new control-channel command pair
(`OperationMessage` / `OperationMessageResponse`).

This mechanism is generic — future operative-to-operative RPC patterns
land on it — and its first consumer is the first-party
[`FromOperative`](operative_init_state_providers.md) init-state provider.

## Configuration

Attach a `%Flezha.Plugin.Driver.MessageHandlerPlugin{}` to a running
operative via `Flezha.Service.PluginManager.enable_plugin/2`:

```elixir
alias Flezha.Plugin.Driver.MessageHandlerPlugin
alias Flezha.Plugin.Provider.MessageForward
alias Flezha.Service.PluginManager

PluginManager.enable_plugin("orders-fetch-init", %MessageHandlerPlugin{
  namespace: "default",
  operative_id: "orders-aggregator",
  handler_key: :fetch_init_state,
  provider: MessageForward,
  opts: %{}
})
```

`handler_key` must be unique per operative. Enabling a second plugin with
the same key returns `{:error, {:handler_key_conflict, key}}` and the
existing handler is left untouched. Conflicts surface at enable-time, not
at dispatch-time, so misconfiguration is loud and deterministic.

## Request envelope

Callers send a fixed BEAM envelope directly to the target operative's
Overwatch pid:

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

`payload` is opaque `binary()`. The target Overwatch looks up the handler
by `handler_key` in O(1) and dispatches the invocation to a Task under
`Flezha.Operative.MessageHandlerTaskSup`. The Task runs
`provider.handle/2`, catches exceptions as
`{:error, {:exception, kind, reason}}`, and sends
`{:flezha_response, ref, result}` back to the caller. Overwatch is not
blocked on the handler — IO casts and other lifecycle messages are still
served while the handler runs.

Unknown `handler_key`:

```elixir
{:flezha_response, ref, {:error, {:unknown_handler_key, key}}}
```

Malformed envelopes (messages that do not match the tuple shape) are
out-of-contract: they fall through the supervisor's normal crash-and-
restart path. The caller's `after timeout_ms` clause is the authoritative
liveness bound — a malformed send still surfaces to the caller as
`{:error, :timeout}`.

## Timeouts

The caller owns the deadline. There is no server-side deadline propagation:
a late reply arriving after the caller's timeout is delivered to the
caller's mailbox and discarded by selective `receive` (the ref no longer
matches).

For chains running inside `Overwatch.init/1` (notably
[`FromOperative`](operative_init_state_providers.md)), the inner
`:request_timeout_ms` must be strictly below the enclosing
`%InitStatePlugin{}.timeout_ms` so the inner `receive` fires first and
returns a clean `{:error, :timeout}` instead of being force-killed by the
outer `Task.shutdown/2`.

## Provider behaviour

A provider is any module implementing
`Flezha.Plugin.MessageHandlerBehaviour`:

```elixir
defmodule MyApp.CachedReply do
  @behaviour Flezha.Plugin.MessageHandlerBehaviour

  @impl true
  def handle(_payload, context) do
    case MyApp.Cache.get(context.operative_id) do
      nil -> {:error, :cache_miss}
      value -> {:ok, value}
    end
  end
end
```

The `context` map contains:

| Key                 | Type                 | Meaning                                          |
| ------------------- | -------------------- | ------------------------------------------------ |
| `operative_id`      | `String.t()`         | Target operative's id                            |
| `namespace`         | `atom() \| String.t()` | Target operative's namespace                   |
| `runner_id` | `String.t()`         | Runner hosting the target operative      |
| `handler_key`       | `atom()`             | The matched handler key                          |
| `from`              | `pid()`              | The caller's pid (use for authorization gating)  |
| `opts`              | `map()`              | The plugin's configured `opts` map               |

Return `{:ok, binary()}` for success or `{:error, term()}` to surface a
structured error.

## First-party `MessageForward` provider

`Flezha.Plugin.Provider.MessageForward` is the canonical handler that
bridges Elixir-side requests to the SDK-side factory. It wraps
`Flezha.Runner.ControlUnit.send_operation_message/3`, which
transmits an `OperationMessage` command on the control channel and awaits
the `OperationMessageResponse`. An empty `error` field maps to
`{:ok, payload_bytes}`; a non-empty `error` maps to `{:error, error_string}`.

### Not to be confused with operative-to-operative messaging

`:on_message` is a **synchronous side-channel query** whose destination is the
SDK factory's `handle_message` callback. To make one operative **feed** another
— asynchronous data flow into the target's durable *input stream* — use
`Flezha.Plugin.Provider.OperativeForward` instead. Different destinations,
different durability, complementary rather than competing:
[`operative_forward_plugin.md`](operative_forward_plugin.md).

## SDK factory contract

The SDK side exposes an optional `handle_message` hook on the factory
interface. Factories that do not override it return an explicit
"unsupported" error, so mis-routing is loud rather than silent.

### Rust

```rust
use flezha_runner::{UnboundedOperative, operative::OperativeError};

struct MyOperative;
impl UnboundedOperative<MyState, String, String> for MyOperative {
    fn process(&mut self, input: String, state: &mut MyState)
        -> Result<String, OperativeError> { /* ... */ }

    fn handle_message(
        &mut self,
        payload: &[u8],
        state: &mut MyState,
    ) -> Result<Vec<u8>, String> {
        // Return opaque reply bytes.
        Ok(serde_json::to_vec(state).unwrap())
    }
}
```

### Java

```java
public final class MyOperative
        implements UnboundedOperative<MyState, String, String> {

    @Override
    public String process(String input, MyState state) { /* ... */ }

    @Override
    public byte[] handleMessage(byte[] payload, MyState state) throws Exception {
        // Return opaque reply bytes.
        return objectMapper().writeValueAsBytes(state);
    }
}
```

Factories that **do not** override the method throw/return the default
"unsupported" error. The SDK converts it to an `OperationMessageResponse`
with a non-empty `error`, so the calling provider sees
`{:error, "operative does not support handle_message"}` (or the
equivalent SDK string) and the operative stays healthy. Existing factories
compile and run unchanged.

## Control-channel command

Two new protobuf messages (see
[sdk_protocol_spec.md](sdk_protocol_spec.md)):

```proto
message OperationMessage {
  string operative_id = 1;
  bytes payload = 2;
}

message OperationMessageResponse {
  bytes payload = 1;
  string error = 2;  // empty on success
}
```

`OperationMessage` is dispatched on the existing control stream via the
same primitives as `SegmentState` — no new stream class, no transport
plumbing. The SDK decodes the request, invokes the factory's
`handle_message`, wraps the result in `OperationMessageResponse`, and
writes it as the outer `CommandResponse.raw_data`.

## Migration

`MessageHandlerPlugin` registrations travel with the operative via the
existing ProcessHub handover path. `Overwatch.alter_handover_state/2`
re-registers each migrated entry via `PluginManager.register_plugin/2` so
the handler is reachable under the same `handler_key` after migration.

Race outcomes when a request is in flight during migration:

| Caller resolves | Caller sends | Outcome |
| --------------- | ------------ | ------- |
| Before handover | Before handover | Normal reply |
| Before handover | After old pid dies | Message dropped → caller times out |
| After registry swap | After registry swap | New pid, handler already re-registered → reply |
| During the swap gap | — | `get_operative_pid/2` returns `nil` → `{:error, :source_operative_not_found}` |

None of these crash the caller, the source Overwatch, or the target
Overwatch. SDK-side roundtrips are unaffected by *operative* migration
because `ControlUnit` lives on the runner, not the operative.

## Caveats and scope

- **Open authorization (v1).** Any cluster-reachable pid can send a
  request; scoping is deferred. A handler plugin's own code may inspect
  `context.from` to gate requests. Real ACLs are a separate change when a
  concrete threat model emerges.
- **Cross-node latency eats the caller's timeout budget.** Chains of
  request/reply across BEAM nodes consume the caller's `timeout_ms`
  budget. Size timeouts to account for distribution round-trip; for
  `FromOperative`, leave headroom under the outer
  `%InitStatePlugin{}.timeout_ms`.
- **Control-channel contention.** Large `OperationMessage.payload`
  values share the control stream with other commands. Keep payloads
  small; if request/reply traffic grows, a dedicated stream class is a
  future enhancement.
- **Not in v1.** Fire-and-forget, publish/subscribe, streaming replies,
  fan-out to multiple handlers per key, rate limiting, deadline
  propagation to the SDK.
