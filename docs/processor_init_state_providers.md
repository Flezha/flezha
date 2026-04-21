# Processor Init-State Providers

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
