# Processor Init-State Providers

> **Note:** the `Flezha.Processor.InitState.Providers.Sibling` provider has
> been **removed**. Use `flz cluster restore` for restart-survival seeding
> — see [`cluster-snapshot.md`](cluster-snapshot.md). The broader
> provider-chain mechanism documented below remains in place for
> non-snapshot init-state sources (HTTP, S3, defaults, etc.); custom
> providers can be added by implementing `on_processor_init/2`.

Init-state providers hydrate a processor's initial state at startup from
sources external to the builder (database, HTTP, computed defaults, etc.).
They are configured per processor, run synchronously inside
`Flezha.Processor.Overwatch.init/1` before the SDK sees the processor, and
feed the resulting map into the existing `init_values` field of the
`AddProcessor` command — no SDK or protocol change is involved.

## Configuration

Attach a list of `%Flezha.Processor.InitStatePlugin{}` structs to a
processor via `Builder.opts[:init_state_plugins]`:

```elixir
alias Flezha.Processor.Builder
alias Flezha.Processor.InitStatePlugin

processor = %Builder{
  id: "orders-42",
  factory_id: "order_aggregator",
  opts: [
    init_state_plugins: [
      InitStatePlugin.new(provider: MyApp.DbInitState, opts: %{table: :orders}),
      InitStatePlugin.new(provider: MyApp.DefaultOverlay, on_error: :empty)
    ]
  ]
}
```

`InitStatePlugin.new/1` raises on missing `:provider`. Other fields default
to `opts: %{}`, `on_error: :fail`, `timeout_ms: 5_000`.

## Provider behaviour

A provider is any module implementing
`Flezha.Processor.InitStateProvider`:

```elixir
defmodule MyApp.DbInitState do
  @behaviour Flezha.Processor.InitStateProvider

  @impl true
  def resolve(context) do
    table = Map.fetch!(context.opts, :table)

    case MyApp.Repo.fetch_state(table, context.processor_id) do
      {:ok, row} -> {:ok, row_to_map(row)}
      :not_found -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp row_to_map(row), do: %{"inputs" => row.inputs, "cursor" => row.cursor}
end
```

The context argument contains exactly these keys:

| Key            | Type       | Meaning                                         |
| -------------- | ---------- | ----------------------------------------------- |
| `processor_id` | `String`   | The starting processor's id                     |
| `namespace`    | `atom`     | The processor's namespace                       |
| `factory_id`   | `String`   | The factory id for the processor                |
| `accumulated`  | `map`      | The shallow-merged result of prior plugins      |
| `opts`         | `map`      | The plugin's own `%InitStatePlugin{}.opts`      |

Return `{:ok, map}` to shallow-merge `map` into the accumulator, or
`{:error, reason}` to trigger the plugin's `on_error` policy.

## Chain semantics

Plugins run in order. Each `{:ok, map}` return is combined with
`Map.merge(accumulator, map)` — keys from later plugins take precedence.
This makes the natural pattern "broad fetch first, specific overlay second"
ergonomic:

```elixir
[
  InitStatePlugin.new(provider: MyApp.DbInitState, opts: %{table: :orders}),
  InitStatePlugin.new(provider: MyApp.DefaultOverlay,
                      opts: %{defaults: %{"retries" => 0}})
]
```

`DbInitState` fetches the persisted row; `DefaultOverlay` fills in keys the
row did not provide. If a later plugin wants to *replace* keys the earlier
one set, it just returns a map including those keys.

## Failure semantics

Each plugin's `on_error` controls what happens on an explicit
`{:error, reason}` return, a timeout, an exception, or an invalid plugin
configuration:

* `:fail` (default) — abort the chain and return `{:error, reason}` from
  `Overwatch.init/1`. The SDK never receives an `AddProcessor` command for
  this processor.
* `:empty` — log at debug and skip this plugin; the accumulator passes
  through unchanged to the next plugin.

Provider exceptions are captured as
`{:exception, kind, reason, stacktrace}` and never crash Overwatch.

## Timeouts

Each plugin has a `timeout_ms` (default `5_000`). Providers run inside an
isolated `Task`; exceeding the timeout forcibly kills the task and treats
it as an error per the plugin's `on_error`. The total chain time is
bounded above by the outer `start_processor_sdk` 8-second GenServer call,
so long chains should size per-plugin timeouts to sum below that budget.

## Cold-start only

The init-state chain runs only when the processor's Builder
`init_values == %{}`. Otherwise the Builder's inline values are used as-is
and no provider is invoked. Migration (`compute_engine_migrate`) never
runs providers — snapshot-or-inline-`init_values` logic is preserved.

## Worked example: DB fetch → default overlay

```elixir
defmodule MyApp.DbInitState do
  @behaviour Flezha.Processor.InitStateProvider

  @impl true
  def resolve(context) do
    case MyApp.Repo.one_by_processor(context.processor_id) do
      nil -> {:ok, %{}}
      row -> {:ok, %{"cursor" => row.cursor, "inputs" => row.inputs}}
    end
  end
end

defmodule MyApp.DefaultOverlay do
  @behaviour Flezha.Processor.InitStateProvider

  @impl true
  def resolve(context) do
    defaults = Map.get(context.opts, :defaults, %{})
    {:ok, Map.merge(defaults, context.accumulated) |> Map.take(Map.keys(defaults))}
  end
end

processor = %Flezha.Processor.Builder{
  id: "orders-42",
  factory_id: "order_aggregator",
  opts: [
    init_state_plugins: [
      Flezha.Processor.InitStatePlugin.new(
        provider: MyApp.DbInitState,
        timeout_ms: 2_000
      ),
      Flezha.Processor.InitStatePlugin.new(
        provider: MyApp.DefaultOverlay,
        opts: %{defaults: %{"retries" => 0, "cursor" => 0}},
        on_error: :empty
      )
    ]
  ]
}
```

On cold start the DB fetch runs first; missing rows return `%{}` and the
overlay supplies defaults. If the DB is down the first plugin's
`on_error: :fail` (default) aborts startup; changing it to `:empty` would
let the defaults alone seed the processor.

## First-party `FromProcessor` provider

`Flezha.Processor.InitState.Providers.FromProcessor` seeds a cold-starting
processor from another running processor. It is the first concrete
consumer of the generic
[processor message-handler](processor_message_handlers.md) routing
mechanism.

```elixir
alias Flezha.Processor.Builder
alias Flezha.Processor.InitStatePlugin
alias Flezha.Processor.InitState.Providers.FromProcessor

from_source =
  InitStatePlugin.new(
    provider: FromProcessor,
    opts: %{
      source_processor_id: "orders-aggregator",
      handler_key: :fetch_init_state,
      request_timeout_ms: 2_000
    },
    on_error: :fail,
    timeout_ms: 5_000
  )

processor = %Builder{
  id: "orders-shadow",
  factory_id: "order_aggregator",
  opts: [init_state_plugins: [from_source]]
}
```

### opts

| Key                    | Type                 | Default                                 | Notes |
| ---------------------- | -------------------- | --------------------------------------- | ----- |
| `:source_processor_id` | `String.t()`         | **required**                            | Target processor's id |
| `:handler_key`         | `atom()`             | **required**                            | Must match a registered `MessageHandlerPlugin` on the source |
| `:source_namespace`    | `atom() \| String.t()` | `context.namespace`                   | Namespace of the source processor |
| `:request_payload`     | `binary()`           | `<<>>`                                  | Opaque bytes handed to the handler |
| `:request_timeout_ms`  | `pos_integer()`      | `4_000`                                 | Kept strictly below the outer `%InitStatePlugin{}.timeout_ms` |

### Return values

- `{:ok, map}` — a JSON-decoded top-level object is shallow-merged into
  the init-state accumulator.
- `{:error, :source_processor_not_found}` — the source was not found in
  the registry (namespace + processor id lookup failed).
- `{:error, :timeout}` — the handler did not reply within
  `:request_timeout_ms`.
- `{:error, {:invalid_reply_payload, :not_a_map}}` — the reply decoded to
  a non-object JSON value (array, string, number, boolean, `null`).
- `{:error, {:invalid_reply_payload, reason}}` — the reply bytes were not
  valid JSON; `reason` describes the decoder failure.
- `{:error, {:missing_opt, key}}` — the plugin's `opts` omits a required
  field.
- Any other `{:error, reason}` from the handler is propagated unchanged —
  the outer `%InitStatePlugin{}.on_error` decides whether to abort or
  continue.

### Handler contract

The source processor must have a `MessageHandlerPlugin` registered under
the chosen `handler_key`. The canonical setup pairs `FromProcessor` on
the consumer side with `Flezha.Plugin.Provider.MessageForward` on the
source side so the actual state assembly happens in the source
processor's SDK factory (via `handle_message`). The handler must return
JSON-encoded bytes that decode to a top-level object compatible with the
consumer's factory state schema.

### Caveats

- **Open authorization (v1).** Any cluster-reachable caller can invoke
  the source's handler. Real ACLs are deferred; a handler plugin's own
  code may inspect `context.from` to gate requests. See
  [processor_message_handlers.md](processor_message_handlers.md) for the
  details.
- **Cross-node latency eats the caller's timeout budget.** Size
  `:request_timeout_ms` to leave headroom under the enclosing
  `%InitStatePlugin{}.timeout_ms` (default 5 000 ms), and size the outer
  `timeout_ms` below the `start_processor_sdk` 8-second ceiling when
  running across BEAM nodes.
- **Mid-migration races degrade cleanly.** A source that migrates while a
  `FromProcessor` call is in flight either succeeds (resolve-and-send
  before handover, or resolve-after-swap), times out (resolve-before,
  send-after-old-pid), or returns `{:error, :source_processor_not_found}`
  (resolve-during-gap). None crash the caller or either Overwatch. The
  outer `on_error` decides the consumer's behavior.
