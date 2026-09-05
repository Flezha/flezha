# Plugin provider lifecycle

For authors of plugin providers: the modules an operative host
(`Flezha.Operative.Overwatch`) attaches through `Flezha.Plugin.Extension.Table`.

**A provider is one declaration plus one row.** `use ProviderBehaviour` names
every option the provider takes and, for a provider that runs a long-lived
process, the worker it runs; `Flezha.Plugin.Providers` holds the id it is
enabled by. Nothing else is edited to add a provider or an option: no supervisor
of its own, no configuration entry, no allow-list, no documented option table
written by hand, no case in the table. Packaging is in
[installation.md](installation.md).

```elixir
defmodule Flezha.Plugin.Provider.Postgres do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      database: [type: :string, required: true, doc: "Database name"],
      port: [type: :int, default: 5432, min: 1, max: 65_535, doc: "TCP port"],
      ssl: [type: :bool, default: false, doc: "TLS to the server"]
    ],
    worker: [module: Postgres.SinkWorker, restart: :permanent]
end
```

An option row takes `type` (`:int`, `:bool`, `:string`, `:enum`, `:pid`, `:json`,
`:map`, `:list`, `:any`), `default`, `required`, `min` / `max`, `values` for an
enum, `internal: true` for a key an operator may not write, `transient: true`
for one that is never persisted, and `doc`. From the rows the contract derives
the struct, the strict allow-list, the conversion table, the refusals, the help
screen and the option table on this provider's page.

## Two phases

| Phase | Meaning |
| --- | --- |
| `:prepared` | `init_provider/1` has run: options validated and normalised. The plugin holds NO process at all. |
| `:running` | The declared worker is started and `activate/1` has run: upstream connections open, handshakes issued, input may flow. |

The host moves every `:prepared` plugin to `:running` with
`Flezha.Plugin.Extension.Table.bulk_activate/2`, and only after
`start_operative_sdk/2` has returned `{:ok, _}`, when the SDK-side operative is
bound and can take input. A plugin on an operative whose SDK side never binds
therefore costs nothing: no socket, no pool, no process.

## Every callback is declared

`Flezha.Plugin.ProviderBehaviour` declares the whole lifecycle, and each
lifecycle callback carries a default body, so the table calls all of them and a
provider writes only what it actually does. Nothing is discovered by probing
exports.

| Callback | Default | Write one when |
| --- | --- | --- |
| `init_provider/1` | the options as given | the provider validates or derives something beyond its rows |
| `worker_specs/1` | one worker keyed by `:plugin_id`, taking the provider's opts | the worker takes a narrower shape, a different key, or there is more than one |
| `activate/1` | nothing to acquire | something must be acquired that is not the declared worker |
| `health/1` | the declared worker alive, or absent, is healthy | the provider can say more than "the process is up" |
| `inspect_state/1` | `nil` | an operator needs to see state the configuration does not show |
| `terminate_provider/1` | `:ok` | the provider holds something that is neither a worker nor an option |

The seven `on_*/2` extension callbacks are what a subscription attaches to; a
provider implements the ones it subscribes to, and its declaration names exactly
those.

## `init_provider/1`

Receives the options already normalised through the declaration: every declared
row present, converted to its type, defaulted, with the required ones checked
and unknown keys already refused. It answers `{:ok, opts}` or
`{:error, reason}`; any other shape is
`{:error, {:invalid_init_provider_return, other}}`.

Put here: what the rows cannot say. A cross-field rule (a timeout that must fit
inside its own interval), a value derived from another (a plugin id defaulted
from a URL), a bound whose floor is an operator-settable duration. Not here: a
process, a connection, a pool, a handshake. **Preparation starts nothing**, for
every provider alike, which is what makes a `:prepared` plugin free.

**A raise, a throw or an exit is a refusal, not an escape.** `Extension.Table`
converts whatever `init_provider/1` fails with into
`{:error, {:init_provider_crashed, provider, kind, payload}}` at the point it
invokes the callback, on the enable path and on the plugin-heal restart path
alike, and logs the formatted exception with its stacktrace at `:error`. Nothing
is hidden, but a provider bug can no longer take the operative host, or the other
plugins on it, down with it.

## The declared worker

A provider that runs a long-lived process declares it:

```elixir
worker: [module: Kafka.ProducerWorker, restart: :permanent, reusable: {Kafka.ProducerWorker, :reusable?}]
```

The table starts it at activation through `Flezha.Plugin.Provider.WorkerSup`, in
a family named after the provider, under the key `worker_specs/1` gives, with
the declared restart value and reuse check. The table tracks the pid, monitors
it, injects it into `health/1`, `inspect_state/1` and `terminate_provider/1`, and
stops it through the same supervisor on disable and on teardown. **No provider
starts, registers or stops a lifecycle process of its own.**

The restart value is the provider's, not the shape's. A sink is `:permanent`,
because a supervisor restart keeps its queued records ("delay, never loss"); a
forward is `:transient`; a source is `:temporary`, because it has nothing to keep
and the convergence loop's ladder is what brings its plugin back.

A provider's dispatch path addresses its worker by the name it is registered
under (`WorkerSup.worker/2`), never by a pid an option carried: a supervisor
restart replaces the pid and a name survives it.

## `activate/1`

Optional, `activate(opts :: map()) :: :ok | {:ok, term()} | {:error, term()}`.
The declared worker is already up when it runs, so a provider whose activation IS
its worker writes none. Implement it when something else must be acquired once
the operative is bound. `opts` is the stored map with `activation_env` merged
last.

A failed activation leaves the plugin `:prepared` AND is returned to the caller:
`bulk_activate/2`'s second element is `[{plugin_id, reason}]` for every plugin
that did not reach `:running`, so a bind can park honestly and a runtime enable
can refuse the one plugin.

## One event per transition

Every phase change emits `[:flezha, :plugin, :phase]` from the table's one
transition function, carrying `%{plugin_id, provider, from, to, reason}`, on the
way in, between phases, and on the way out.

## Subscription sub-options

`opts` is declared per dispatch kind in `Flezha.Plugin.Extension.Table`, which is
the sole authority on which sub-options exist and what type each carries:

| Kind | Sub-options |
| --- | --- |
| `:fold` | `position` (integer), `timeout_ms` (integer), `on_error` (`"fail"` or `"empty"`) |
| `:transform` | `position` (integer) |
| `:request` | `key` (string) |
| `:timer` | `interval_ms` (integer) |
| `:fanout` | — |

`Extension.Table.canonicalise/2` types the whole declaration before it is
validated, so the same subscription can be written from Elixir, from
`--extension=NAME --NAME.key=value`, from a `--plugins` spec or from an
installed package manifest and mean the same thing: an extension name may be
text or an atom and `opts` a map or a keyword list, and every value is coerced
to its declared type. What the table stores — and what `flz plugin enable`
persists as declared intent — is the canonical `{atom, keyword}` form.

Nothing an operator types is interned. A name that resolves to no extension is
`{:error, {:unknown_extension, name}}`, a sub-option the kind does not declare
is `{:error, {:unknown_subscription_opt, kind, name}}` (`position` on an
unordered kind keeps its own `{:error, :position_not_applicable}`), and a value
that cannot be coerced is `{:error, {:invalid_subscription_opts, {name,
value}}}`. All three are refused at declaration time, before any durable record
of the plugin exists.

## The refusal grammar

A value the declaration cannot accept is refused in one sentence shape, on the
CLI path (`flz plugin enable ... --Xkey=value`) and on the `--plugins` spec path
alike, and the two answer identically:

| The row says | The refusal |
| --- | --- |
| `required: true` and nothing was written | `<provider> requires :<option>` |
| a type the value does not satisfy | `Invalid value for <option>: <value>` |
| `min:` | `<option> <value> is below the minimum <min>` |
| `max:` | `<option> <value> is above the maximum <max>` |
| no row at all | `unknown option(s) for <provider>: <key>` |

A cross-field rule a row cannot express stays in the provider and answers the
same `{:error, binary()}` shape.

A provider may still convert a shape by hand: a private `convert_value/2` clause
runs first, and anything none of its clauses match falls through to the row's
declared type. That is what keeps a JSON option's decode failure a string rather
than a decoder struct, and what closes a two-value frame type the row cannot
name.

| Shared helper | Accepts |
| --- | --- |
| `parse_int/2` | an integer, or a string naming one |
| `parse_bool/2` | a boolean, or `"true"` / `"false"` |
| `parse_enum/3` | a member of a closed set, or its string spelling |
| `parse_pid/2` | a pid, or a registered name |

All four are total: anything else is `{:error, message}` naming the option and
the value. There is deliberately no raising counterpart. An option reaching a
provider is operator-supplied and unvalidated, so a helper that raises on a shape
it did not expect turns a bad value into a crash instead of a refusal, and a
closed set resolved with `String.to_existing_atom/1` is the same defect twice
over, raising for one typo and answering a wrong-but-existing atom for the next.

## Host identity

`init_provider/1` receives, merged last so stored config cannot spoof them:
`:source_namespace` and `:source_operative_id` (the operative the plugin is enabled on) and
`:source_plugin_id` (for a provider that persists its own committed state). `activate/1`
receives the bind-time operative facts the same way (`activation_env`, currently
`:effectively_once`). `OperativeForward` uses the identity to refuse a self-target or a
cross-namespace target at enable time.

## Introspection

```elixir
Flezha.Plugin.Extension.Table.phase(table, plugin_id)      #=> :prepared | :running | {:down, reason} | :unknown
Flezha.Plugin.Extension.Table.running?(table, plugin_id)   #=> the plugin holds its upstream
Flezha.Plugin.Extension.Table.subscribed?(table, plugin_id) #=> the plugin is declared here at all
Flezha.Plugin.Extension.Table.health(table)                #=> %{plugin_id => :healthy | {:unhealthy, reason}}
```

Read a predicate, never the atom: the phase vocabulary is the table's, and the
conformance test fails a comparison against it written anywhere else.

Only `:running` plugins are probed; a `:prepared` one reports `:healthy`. A
provider that overrides nothing is healthy while its declared worker is alive, or
absent. A `health/1` that raises or exceeds
`ProviderBehaviour.probe_budget_ms/0` (500 ms) reads as `:healthy`: convergence
never escalates on an uncertain probe. An unhealthy verdict is what convergence
restarts the plugin on (`FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD`, default 2).

## In-tree providers

Every row of `Flezha.Plugin.Providers`, and what each one declares:

| Provider id | Worker | Restart | Overrides |
| --- | --- | --- | --- |
| `operative_websocket` | `Websocket.Client` | `:temporary` | `init_provider/1`, `worker_specs/1`, `health/1`, `inspect_state/1` |
| `operative_http_poll` | `HttpPoll.PollWorker` | `:temporary` | `init_provider/1`, `worker_specs/1`, `health/1` |
| `operative_kafka_producer` | `Kafka.ProducerWorker` | `:permanent` | `init_provider/1`, `worker_specs/1`, `health/1` |
| `operative_postgres` | `Postgres.SinkWorker` | `:permanent` | `init_provider/1`, `worker_specs/1`, `health/1` |
| `operative_forward` | `OperativeForward.ForwardWorker`, one per target member | `:transient` | `init_provider/1`, `worker_specs/1`, `health/1` |
| `monitored_io_forward` | itself, a heartbeat GenServer | `:temporary` | none |
| `operative_io_filter` | none | | `init_provider/1` |
| `operative_stdout`, `operative_beam_msg`, `operative_message_forward` | none | | none |

## Shared helpers for queue-holding workers

Providers that buffer records towards a slow destination (`Postgres.SinkWorker`,
`OperativeForward.ForwardWorker`) share one supervision core and the policy helpers in
`Flezha.Plugin.ProviderBehaviour`:

| Helper | Purpose |
| --- | --- |
| `Flezha.Plugin.Provider.WorkerSup` | Registry + DynamicSupervisor per provider family: `ensure_worker/4` (reuse while `reusable?` holds, else reap and restart; an existing worker that still fails the check is `{:error, {:not_reusable, pid}}`, never handed back), `stop_worker/2` |
| `Flezha.Config.Backoff.arm/4` | the wait before a retry: the provider's row of the ladder table, with the worker's own `base`/`cap` where those are plugin options. No provider computes a delay of its own |
| `saturated?/1`, `park_or_reply/2`, `release_waiters/1` | the "delay, never loss" overflow policy over `:buffer_len`, `:queue_max`, `:waiters` in the worker state |
| `worker_health/3` | the `health/1` preamble: absent / dead verdicts, else the provider's verdict computed off the caller under the probe budget |
| `ask/3` | a bare `{tag, from, ref}` probe for workers a `GenServer.call` must not queue behind |

The Kafka producer bounds its queue too but offers drop modes, so it keeps its own overflow
handling. Two rules:

- **Never deliver from the worker's own `handle_call`.** A worker blocked on a downstream
  call stops servicing enqueues and back-pressures into the source Overwatch; when the
  destination is another forwarding operative that is a four-way deadlock. `ForwardWorker`
  spawns a monitored process per in-flight item for this reason.
- **Choose `restart:` deliberately.** A disable stops the owned pid with `:shutdown`. A
  `:permanent` child is restarted by its DynamicSupervisor and outlives the disable; a
  `:transient` one stays down. `ForwardWorker` and `ProducerWorker` are `:transient`
  because disabled must mean stopped.

## Enabling: claim, then subscribe

`Flezha.Service.PluginManager` canonicalises and type-checks the subscriptions against the
host catalog first, so an ill-typed, undeclared or unknown one is refused before any
durable record exists and the convergence loop is never handed intent that can never be
satisfied. It then serialises only the id claim; the caller writes the cspec row and
subscribes on the host, and a failed enable releases the claim. Ids whose enable is
in flight are reported by `PluginManager.pending_ids/0` and are not healed by convergence.

Re-enabling an id already registered for the **same** operative in the same namespace is a
no-op that exits 0: no cspec write, no re-subscribe, the live worker keeps running. Options
are not compared — changing opts means disable + enable. The same id against a different
target is an error naming the holder. Against a partition group, already-enabled members
count as success and the summary reports enabled vs already-enabled counts; a failing
member is a loud error. A cold-start script can therefore re-run its `flz plugin enable`
lines unconditionally.

## Teardown

`Flezha.Plugin.Extension.Table.teardown_all/1` and a disable handle both phases
uniformly, and phase is no precondition: `terminate_provider/1` runs, every worker
the table started is stopped through the supervisor that started it, timers are
cancelled, monitors released, the `PluginManager` registration dropped. A
`:prepared` plugin holds no worker, so its teardown is the same call that empties
a `:running` one.

The stop reaches the DECLARED keys as well as the tracked ones. A `:transient` or
`:permanent` worker that crashed was restarted by its own supervisor under the
same key while the table dropped it from the record, and stopping only what is
tracked would leave that replacement running for ever.

`terminate_provider/1` is for what is neither a worker nor an option, and must be
idempotent: a disable followed by a stop calls it twice.

## See also

- [installation.md](installation.md) — packaging, manifests and the `flz plugin` verbs
- [operative_forward.md](operative_forward.md) — the fan-out provider with one worker per member
- [http_poll.md](http_poll.md) — the smallest two-phase source provider
- [postgres_sink.md](postgres_sink.md) — the smallest declared-worker sink
- [../internals/convergence_loop.md](../internals/convergence_loop.md) — how unhealthy plugins are healed
