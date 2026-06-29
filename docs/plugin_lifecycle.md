# Plugin lifecycle: `init_provider/1` vs `activate/1`

Plugin providers attached to an operative host (e.g. `Flezha.Operative.Overwatch`)
move through two phases:

- **`:prepared`** — the provider's GenServer is registered, config is
  validated, struct is constructed. The provider is callable but has
  not been instructed to acquire upstream resources.
- **`:running`** — upstream connections are opened, subscribe
  handshakes issue, downstream consumers attach. Input may flow.

The transition is driven by `Flezha.Plugin.Extension.Table.bulk_activate/1`,
invoked by the operative host **only** after `start_operative_sdk/2` returns
`{:ok, _}` — i.e. the SDK-side operative is bound and ready to accept
input. Plugins that finish `init_provider/1` before the SDK is ready
remain in `:prepared` and emit no `:io_in` until activation.

The split exists because the controller's operative-startup path is a
multi-step handshake (resolve executor → ping → AddOperative) that can fail
or stall. Provider code that opens upstream connections during
`init_provider/1` runs even when the operative never becomes
`:running` — every retry cycle re-opens the connection, every
incoming frame queues into an operative that cannot drain to the SDK, and
the controller's process memory grows without bound. This was the root
cause of the 2026-05-15 production incident (`fix-overwatch-restart-storm`).

## What to put in `init_provider/1`

- Config validation (required keys, type coercion).
- GenServer registration via `start_link` of a LOCAL process (a struct
  store, a worker supervisor, a metrics collector).
- Struct construction, returning `{:ok, opts}` or `{:ok, opts, pid}`.
- Anything fast (< ~100 ms), deterministic, and local.

## What to put in `activate/1`

`activate/1` is an **optional** callback. Implement it when your
provider needs to do any of the following:

- Open a TLS or plain TCP connection to an upstream broker, websocket
  endpoint, or HTTP host.
- Issue a protocol-level handshake (websocket subscribe, Kafka API
  version negotiate, NATS subscription).
- Acquire a resource whose health depends on the operative's SDK side
  being ready.

The callback signature is `activate(opts :: map()) :: :ok | {:ok, pid()} | {:error, term()}`.

When a `{:ok, pid}` value is returned the extension table monitors
`pid` and treats it as a subscriber-owned process; a crash triggers
the existing restart machinery
(`Flezha.Plugin.Extension.Table.handle_down/5`). Restarts re-enter
the `:prepared` → `:running` transition, so `activate/1` runs again on
every restart.

## What NOT to put in `init_provider/1`

- TLS handshakes against remote hosts.
- Subscribe-style protocol handshakes that solicit upstream traffic.
- Long blocking calls (the host's `dist_sup` is waiting on
  `start_link`).
- Anything that assumes the operative's SDK side is bound — at
  `init_provider/1` time it is not.

## In-tree examples

| Provider | `init_provider/1` body | `activate/1` body |
| --- | --- | --- |
| `Plugin.Provider.Websocket` | normalises opts, validates URI | opens the Fresh WS connection and runs the subscribe handshake |
| `Plugin.Provider.Kafka.Producer` | starts a local `ProducerSup` worker | not needed — broker connection is lazy on first publish |
| `Plugin.Provider.Filter` | validates filter rules | not needed — work runs on `:on_input` / `:on_output` |
| `Plugin.Provider.BeamMsgForward`, `MessageForward`, `MonitoredIoForward`, `StdOut`, `Metrics` | stores config | not needed — naturally quiescent until input arrives |

## Phase introspection

```elixir
Flezha.Plugin.Extension.Table.phase(table, plugin_id)
#=> :prepared | :running | :unknown
```

`:unknown` indicates the plugin is not registered on the table.

## Teardown

`Flezha.Plugin.Extension.Table.teardown_all/1` handles both phases
uniformly — owned pids are killed, timers are cancelled, monitors are
demonitored, and `PluginManager` registrations are released regardless
of phase. Provider authors do NOT need to write phase-aware cleanup
code.
