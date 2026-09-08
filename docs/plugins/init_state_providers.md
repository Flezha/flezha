# Init-state providers

For plugin authors seeding an operative's initial state from outside the
builder — a database row, an HTTP resource, computed defaults — and for
operators attaching such a plugin. An init-state provider is an ordinary
`%Flezha.Plugin{}` subscribed to the `:on_operative_init` extension point. The
chain runs on the Overwatch, inside the bind and before the SDK is asked to
start the operative, and its result becomes the operative's `init_values`; no
SDK or wire change is involved. This doc covers how the chain is attached, the
provider callback, the merge order, failure and timeout semantics, and when the
chain runs at all.

## Attaching the chain

`:on_operative_init` plugins are attached **at operative start** only, through
`Builder.opts[:plugins]` or `flz operative start --plugins`.
`PluginManager.enable_plugin/2` refuses a plugin carrying that subscription
with `"init-state plugins must be attached at operative start"` — there is
nothing left to seed once the operative is up.

```elixir
alias Flezha.Operative.Builder
alias Flezha.Plugin

operative = %Builder{
  id: "orders-42",
  factory_id: "order_aggregator",
  opts: [
    plugins: [
      Plugin.new(
        id: "db-seed",
        provider: MyApp.DbInitState,
        opts: %{table: :orders},
        subscriptions: [{:on_operative_init, [timeout_ms: 2_000]}]
      ),
      Plugin.new(
        id: "defaults",
        provider: MyApp.DefaultOverlay,
        opts: %{defaults: %{"order_aggregator_procedure" => %{"retries" => 0}}},
        subscriptions: [{:on_operative_init, [on_error: "empty"]}]
      )
    ]
  ]
}
```

The same two plugins on the CLI, where a provider is named by its registered
provider id and subscription opts arrive as strings — each is coerced to the
type its option declares:

```bash
flz operative start orders-42 order_aggregator --plugins '[
  {"id": "db-seed", "provider": "my_db_init_state", "opts": {"table": "orders"},
   "subscriptions": [["on_operative_init", {"timeout_ms": 2000}]]},
  {"id": "defaults", "provider": "my_default_overlay",
   "opts": {"defaults": {"order_aggregator_procedure": {"retries": 0}}},
   "subscriptions": [["on_operative_init", {"on_error": "empty"}]]}
]'
```

`flz plugin help <provider>` lists `on_operative_init` under "SUPPORTED
EXTENSIONS" when the provider exports the callback.

### Subscription options

| Option | Default | Meaning |
|---|---|---|
| `timeout_ms` | `5_000` | per-provider budget; a positive integer |
| `on_error` | `"fail"` | `"fail"` aborts the start, `"empty"` skips this provider |
| `position` | append order | explicit place in the chain; a non-negative integer |

Each option is coerced to its declared type, so `"2000"` and `2000` mean the
same thing from every entry point. A value that cannot be coerced fails the
subscribe — and so the start — with `{:invalid_subscription_opts, {option,
value}}`, and an option `:fold` does not declare fails with
`{:unknown_subscription_opt, :fold, name}`.

## Provider callback

A provider exports `on_operative_init/2`. It receives the chain's context and
the plugin's own `opts`, and returns the context with `accumulated` updated:

```elixir
defmodule MyApp.DbInitState do
  alias Flezha.Plugin.Extension.Ctx

  def on_operative_init(%Ctx.Init{} = ctx, opts) do
    table = Map.fetch!(opts, :table)

    case MyApp.Repo.fetch_state(table, ctx.operative_id) do
      {:ok, row} -> {:ok, %Ctx.Init{ctx | accumulated: row_to_map(row)}}
      :not_found -> {:ok, ctx}
      {:error, reason} -> {:error, reason}
    end
  end

  # The accumulator IS `init_values`, so its top level is Procedure names.
  defp row_to_map(row) do
    %{"order_aggregator_procedure" => %{"inputs" => row.inputs, "cursor" => row.cursor}}
  end
end
```

The map a provider returns is the operative's `init_values`, so it takes the
procedure-keyed shape every state blob takes: `%{procedure => state}` for a
single-Entity Procedure and `%{procedure => %{entity_key => state}}` for a
statically keyed one. A top-level key that is not a Procedure the class carries
is ignored by the runner and the operative starts with default state, so a
provider that returns a flat map of domain fields seeds nothing. The shape and
what the runner does with each entry are in
[`../sdk/protocol_spec.md`](../sdk/protocol_spec.md) "Operative state JSON
shape".

`%Flezha.Plugin.Extension.Ctx.Init{}` carries exactly:

| Field | Type | Meaning |
|---|---|---|
| `operative_id` | `String.t()` | the starting operative's id |
| `namespace` | `atom() \| String.t()` | the operative's namespace |
| `factory_id` | `String.t()` | the operative's factory id |
| `accumulated` | `map()` | the shallow-merged result of prior providers |

Return `{:ok, %Ctx.Init{}}` or `{:error, reason}`. Any other return — a bare
`{:ok, map}` included — is `{:error, {:invalid_return, value}}` and follows
the provider's `on_error` policy.

## Chain semantics

Providers run in `position` order. After each `{:ok, ctx}` the host merges
`Map.merge(previous.accumulated, returned.accumulated)`, so a provider may
return only the keys it adds and keys from later providers take precedence.
The natural pattern is "broad fetch first, specific overlay second"; a later
provider that wants to replace a key returns a map containing it. The final
`accumulated` map becomes the operative's `init_values`, which the SDK
receives in `AddOperative`.

The merge is one level deep and its keys are Procedure names, so two providers
seeding the same Procedure do not combine: the later one's state replaces the
earlier one's whole. Providers that each own a different Procedure compose
cleanly; one that wants to overlay a field on another's state has to read the
accumulator and rebuild that Procedure's entry itself.

## Failure semantics

Each provider runs in an isolated `Task`. Its `on_error` decides what an
explicit `{:error, reason}`, an invalid return, a timeout, an exception or a
task exit does:

| `on_error` | Effect |
|---|---|
| `"fail"` (default) | the chain halts with `{:error, reason}` and the start fails — `OperativeManager.start_operative/3` returns `{:error, %{reason: reason, …}}` and the SDK never receives `AddOperative` |
| `"empty"` | logged at debug; the accumulator passes through unchanged to the next provider |

A provider that exceeds `timeout_ms` is killed (`Task.shutdown/2`,
`:brutal_kill`) and reported as `:timeout`; an exception is captured as
`{:exception, kind, reason, stacktrace}` and never crashes the Overwatch. The
chain runs before the SDK is asked to start the operative and its total is the
sum of the providers' budgets, so size per-provider timeouts for the start
latency you can accept.

## Cold start only

The chain runs only when the Builder's `init_values == %{}`. Non-empty inline
values are used as-is and no provider is invoked. A runner migration
(`:runner_migrate`) and a ProcessHub handover never run providers: per-Entity
state is restored from its own WAL and snapshot on bind, and the operative set
is replayed from the durable registry on a cold boot — see
[`../internals/state_persistence.md`](../internals/state_persistence.md) and
[`../operations/auto_restore_on_boot.md`](../operations/auto_restore_on_boot.md).
Restart survival is therefore never an init-state concern; the chain is for
genuine external seeds.

No first-party init-state provider ships. A provider that needs another
operative's state can send that operative the `:on_message` request envelope
and merge the reply — see [`message_handlers.md`](message_handlers.md) —
keeping its request timeout below its own `timeout_ms` so it answers cleanly
instead of being killed.

## See also

- [`message_handlers.md`](message_handlers.md) — querying a running operative
- [`lifecycle.md`](lifecycle.md) — `init_provider/1` runs before the chain; what belongs where
- [`../internals/state_persistence.md`](../internals/state_persistence.md) — why restarts need no init-state provider
