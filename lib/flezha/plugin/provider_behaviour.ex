defmodule Flezha.Plugin.ProviderBehaviour do
  @moduledoc """
  The plugin provider contract, and the shared helpers every provider uses to
  honour it.

  A provider is ONE declaration, of a relay plugin (`type: :relay`, the
  default), which acts on the records passing through the relays it is bound
  to, or of a controller plugin (`type: :controller`), which is bound to none
  and is told what happened in its namespace. `use ProviderBehaviour, options:
  rows, worker: spec` names every option the provider takes — type, default, whether it is
  required, its bounds, its documentation — and, for a provider that runs a
  long-lived process, the worker module and its restart value. From the rows
  the `use` derives `defstruct`, `@enforce_keys`, `build/1`, `help/1`,
  `provider_info/0`, `validate_opts/1` and the conversion table; from the worker
  spec, `Flezha.Plugin.Extension.Table` starts, tracks, probes and stops the
  process at activation.

  Every callback the host can invoke is declared here, and the lifecycle ones
  carry a default body a provider overrides only when it has something to do.
  `__before_compile__` records what each provider actually implements in
  `__provider__/0`, so the host reads a compile-time declaration where it used
  to probe exports at run time.
  """

  defmodule Decl do
    @moduledoc """
    One provider's compile-time declaration, as `__provider__/0` answers it.

    `extensions` are the `on_*/2` callbacks the module defines and `own` the
    lifecycle callbacks it overrode — both read from the module while it
    compiles, which is the same source a run-time export probe reads and one the
    host can trust without asking the code loader.

    `feeds` is the one fact the module cannot be read for: whether the provider's
    own worker injects payloads into the relay. It is declared, because a
    callback list cannot say it — see `channel_jobs/2`. `shareable` is the other:
    whether one running copy can serve several bound relays, which a file
    reader with one offset or a server on one port cannot. `telemetry` names the
    verbs a provider emits beyond the shared vocabulary, each a metric or an
    event, which the telemetry bridge exports under the provider's name; `emits`
    the shared verbs a first-party provider writes, each with the level its event
    is also logged at, or nil; `title` and `description` what a reader of its
    source is told. `namespace` is the one namespace the provider may be enabled
    in, or nil for any.
    """

    @type worker :: %{
            module: module(),
            restart: :permanent | :transient | :temporary,
            reusable: {module(), atom()} | nil
          }

    @type t :: %__MODULE__{
            module: module(),
            name: binary(),
            type: atom(),
            options: [{atom(), keyword()}],
            extensions: [atom()],
            own: [atom()],
            worker: worker() | nil,
            telemetry: [{atom(), :metric | :event | {:metric | :event, String.t()}}],
            emits: [{atom(), Logger.level() | nil}],
            title: String.t() | nil,
            description: String.t() | nil,
            feeds: boolean(),
            shareable: boolean(),
            namespace: binary() | nil
          }

    defstruct [
      :module,
      :name,
      :type,
      :worker,
      :namespace,
      :title,
      :description,
      options: [],
      extensions: [],
      own: [],
      telemetry: [],
      emits: [],
      feeds: false,
      shareable: false
    ]

    @doc "The rows a user can write: everything but the internal ones."
    @spec public_options(t()) :: [{atom(), keyword()}]
    def public_options(%__MODULE__{options: options}),
      do: Enum.reject(options, &Keyword.get(elem(&1, 1), :internal, false))

    @doc """
    The rows held in memory and never persisted: a resolved credential, or
    anything else a stored spec must not carry across a restart of the node.
    """
    @spec transient_options(t()) :: [atom()]
    def transient_options(%__MODULE__{options: options}),
      do: for({name, row} <- options, Keyword.get(row, :transient, false), do: name)

    @doc """
    The hooks on which the provider acts on records passing through a
    relay, of those it defines: what any binding of a plugin may hook it on.
    """
    @spec io_hooks(t()) :: [atom()]
    def io_hooks(%__MODULE__{extensions: extensions}),
      do: Enum.filter(extensions, &(&1 in Flezha.Plugin.ProviderBehaviour.io_callbacks()))

    @doc "The rows a running plugin applies in place through `reconfigure/2`."
    @spec live_options(t()) :: [atom()]
    def live_options(%__MODULE__{options: options}),
      do: for({name, row} <- options, Keyword.get(row, :live, false), do: name)
  end

  @typedoc "Health verdict for a running provider instance."
  @type health_verdict() :: :healthy | {:unhealthy, reason :: term()}

  @typedoc """
  A verdict plus the third answer a probe can give: `{:fail_open, reason}` for a
  probe that could not be completed. It reads as healthy — the reconciler must
  never escalate on uncertainty — but it is NOT a verdict, and the layer that
  knows the plugin's identity turns it into telemetry (`Extension.Table`).
  """
  @type probe_result() :: health_verdict() | {:fail_open, reason :: term()}

  @typedoc """
  One worker the table starts for a provider: the registry key it takes inside
  the provider's family, and the opts it is started with.
  """
  @type worker_spec() :: {key :: term(), opts :: map() | keyword()}

  alias Flezha.Config.Durations

  @callback plugin_type() :: :relay | :controller
  @callback build(map()) :: {:ok, struct()} | {:error, term()}

  @doc """
  The screen `flz plugin help <provider_id>` prints. Generated by `use` from the
  option rows; a provider overrides it for custom help text.
  """
  @callback help(provider_id :: String.t()) :: String.t()

  @callback provider_info() :: map()

  @doc """
  The provider's compile-time declaration. Generated by `use`; the host reads it
  instead of probing which callbacks the module exports.
  """
  @callback __provider__() :: Decl.t()

  @doc """
  Validate and normalise the plugin's options. Deliberately performs NO upstream
  I/O and starts no process — a declared worker is started by the table at
  activation, so a plugin on a relay whose SDK side never binds holds
  nothing.

  Default: the options as given.
  """
  @callback init_provider(opts :: map()) ::
              {:ok, map()} | {:ok, map(), pid() | nil} | {:error, term()}

  @doc """
  Acquire the provider's upstream resources — TLS connect, subscribe handshake,
  downstream attach — after the relay's SDK side has confirmed bound.

  Default: nothing to acquire. A declared worker is already started by the table
  before this is called, so a provider whose activation IS its worker overrides
  nothing.
  """
  @callback activate(opts :: map()) :: :ok | {:ok, term()} | {:error, term()}

  @doc """
  The workers the table starts for this provider at activation, as
  `{registry_key, worker_opts}` — one for a provider with one worker, one per
  target for a provider that fans out.

  Default: one worker keyed `WorkerSup.key/1`, started with the provider's own
  opts. A provider whose worker takes a narrower shape, or needs more than one,
  declares them here and nowhere else: this alone names what the table starts.
  """
  @callback worker_specs(opts :: map()) :: [worker_spec()]

  @doc """
  Optional health verdict for a *running* provider instance. The reconciler
  (`Flezha.Plugin.Extension.Table.health/1`) queries this to escalate — restart
  the provider under a bounded governor — only after the provider's own fast
  in-process recovery has repeatedly failed. `opts` is the provider's stored
  opts with `:owned_pid` (the process the table started, if any) and `:workers`
  (every one of them, by key) injected by the host.

  Default: the owned pid being alive, or absent, is healthy. Providers SHOULD
  reuse their existing liveness state rather than introduce new timers (e.g. the
  WebSocket provider reuses `last_frame_at`/`last_data_frame_at`).
  """
  @callback health(opts :: map()) :: health_verdict()

  @doc """
  Optional user-facing detail for a *running* provider instance, rendered
  by `flz relay info` beneath the plugin that reported it. `opts` is the
  provider's stored opts with `:owned_pid` injected, exactly as for `health/1`.

  This is for state a user must be able to see and the plugin's static
  configuration does not show — the websocket provider's applied subscription
  set, the outcome of its last reconcile, its last error. Default `nil`, so a
  provider stays silent for the instances it does not apply to.

  A report with `connected: false` says the upstream is not open yet: the new
  copy of a `single` plugin waits for it to clear before it replaces the old
  one, so the old connection stays open until the new one is.
  """
  @callback inspect_state(opts :: map()) :: map() | nil

  @doc """
  Optional teardown for a provider that holds something the table cannot see.
  The declared workers are stopped by the table on both exits — a plugin disable
  and a relay teardown — so this is only for what is neither a worker nor
  an option.

  Default `:ok`. It must be idempotent, since a disable followed by a stop calls
  it twice.
  """
  @callback terminate_provider(opts :: map()) :: :ok

  @doc "Dispatch callbacks. A provider implements the ones it can be hooked on."
  @callback on_relay_init(ctx :: struct(), opts :: map()) :: term()
  @callback on_input(ctx :: struct(), opts :: map()) :: term()
  @callback on_output(ctx :: struct(), opts :: map()) :: term()
  @callback on_interval(ctx :: struct(), opts :: map()) :: term()
  @callback on_message(ctx :: struct(), opts :: map()) :: term()
  @callback on_snapshot(ctx :: struct(), opts :: map()) :: term()
  @callback on_terminate(ctx :: struct(), opts :: map()) :: term()
  @doc "A part or a dead-letter notice, for listeners only: no sink implements it."
  @callback on_preview(ctx :: struct(), opts :: map()) :: term()
  @doc "A call the relay took, or how one ended, for listeners only: no sink implements it."
  @callback on_call(ctx :: struct(), opts :: map()) :: term()

  @doc """
  Controller hooks: something happened in the plugin's namespace. Each takes the
  event `Flezha.Observability.HookSink` built (a map with `hook`, `namespace`,
  `controller`, `at`, `text` and the hook's own fields) and returns quickly; a
  provider that does slow I/O hands the event to its own worker.
  """
  @callback on_relay_started(event :: map(), opts :: map()) :: term()
  @callback on_relay_stopped(event :: map(), opts :: map()) :: term()
  @callback on_relay_failed(event :: map(), opts :: map()) :: term()
  @callback on_dead_letter(event :: map(), opts :: map()) :: term()
  @callback on_plugin_unhealthy(event :: map(), opts :: map()) :: term()

  @doc """
  Apply changed `live:` options to the running plugin, given its running
  options and the new ones. Required of a provider that declares a live option.
  """
  @callback reconfigure(old :: map(), new :: map()) :: :ok | {:error, term()}

  @lifecycle_callbacks [
    init_provider: 1,
    activate: 1,
    worker_specs: 1,
    health: 1,
    inspect_state: 1,
    terminate_provider: 1
  ]

  @extension_callbacks [
    :on_relay_init,
    :on_input,
    :on_output,
    :on_interval,
    :on_message,
    :on_snapshot,
    :on_terminate,
    :on_preview,
    :on_call
  ]

  @controller_callbacks [
    :on_relay_started,
    :on_relay_stopped,
    :on_relay_failed,
    :on_dead_letter,
    :on_plugin_unhealthy
  ]

  @optional_callbacks [
    init_provider: 1,
    activate: 1,
    worker_specs: 1,
    health: 1,
    inspect_state: 1,
    terminate_provider: 1,
    on_relay_init: 2,
    on_input: 2,
    on_output: 2,
    on_interval: 2,
    on_message: 2,
    on_snapshot: 2,
    on_terminate: 2,
    on_preview: 2,
    on_call: 2,
    on_relay_started: 2,
    on_relay_stopped: 2,
    on_relay_failed: 2,
    on_dead_letter: 2,
    on_plugin_unhealthy: 2,
    reconfigure: 2
  ]

  @doc "The lifecycle callbacks a provider may override, as `{name, arity}`."
  @spec lifecycle_callbacks() :: keyword(arity())
  def lifecycle_callbacks(), do: @lifecycle_callbacks

  @doc "The dispatch callbacks a relay plugin's hook can name."
  @spec extension_callbacks() :: [atom()]
  def extension_callbacks(), do: @extension_callbacks

  @doc "The hooks a controller plugin can take: what happened in its namespace."
  @spec controller_callbacks() :: [atom()]
  def controller_callbacks(), do: @controller_callbacks

  # The two that carry a payload through the chain, both taking a `Ctx.IO`.
  @io_callbacks [:on_input, :on_output]

  @doc "The dispatch callbacks that carry a payload, in either direction."
  @spec io_callbacks() :: [atom()]
  def io_callbacks(), do: @io_callbacks

  @doc """
  Shared `convert_value/2` result helpers: CLI plugin opts arrive as
  strings, direct (test/Elixir) opts as native types — accept both.
  """
  def parse_int(value, _field) when is_integer(value), do: {:ok, value}

  def parse_int(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> {:ok, int_val}
      _ -> refuse_value(field, value)
    end
  end

  def parse_int(value, field), do: refuse_value(field, value)

  def parse_bool(value, _field) when is_boolean(value), do: {:ok, value}
  def parse_bool("true", _field), do: {:ok, true}
  def parse_bool("false", _field), do: {:ok, false}
  def parse_bool(value, field), do: refuse_value(field, value)

  @doc """
  A process of this VM, as a pid. Anything else is refused, a registered name
  included, so no caller can direct a plugin's messages at a controller process.
  """
  @spec parse_pid(term(), atom() | binary()) :: {:ok, pid()} | {:error, term()}
  def parse_pid(value, _field) when is_pid(value), do: {:ok, value}
  def parse_pid(value, field), do: refuse_value(field, value)

  @doc """
  A closed set of atoms, in either spelling: the option's own atom, or the
  string a CLI or a stored spec carries it as. Anything else is refused, which
  is what makes an enum option total — `String.to_existing_atom/1` in its place
  raises for one typo and answers a wrong-but-existing atom for the next.
  """
  @spec parse_enum(term(), atom() | binary(), [atom() | binary()]) ::
          {:ok, atom() | binary()} | {:error, term()}
  def parse_enum(value, field, allowed) do
    case Enum.find(allowed, &member?(&1, value)) do
      nil -> refuse_value(field, value)
      member -> {:ok, member}
    end
  end

  defp member?(member, value) when member === value, do: true

  defp member?(member, value) when is_atom(member) and is_binary(value),
    do: Atom.to_string(member) === value

  defp member?(member, value) when is_binary(member) and is_atom(value) and not is_nil(value),
    do: member === Atom.to_string(value)

  defp member?(_member, _value), do: false

  @doc """
  The one refusal a conversion table answers: it names the option and the value
  it was given. Argument order mirrors `convert_value/2`, whose injected last
  clause delegates here, rather than the `parse_*` helpers' own `(value, field)`.

  The value rides as given, so a caller reads back what was written.
  """
  @spec refuse_value(atom() | binary(), term()) ::
          {:error, {:invalid_option_value, term(), term()}}
  def refuse_value(field, value), do: {:error, {:invalid_option_value, field, value}}

  @doc """
  Does this plugin have a hook on a direction that carries a payload — `on_input`
  or `on_output`?

  Read from the host-injected `:subscribed_extensions`, so a provider that
  allocates only for the directions it is bound in asks one question instead of
  inferring an answer from a configured option, every one of which has a default.
  """
  @spec io_bound?(map()) :: boolean()
  def io_bound?(opts) do
    Enum.any?(Map.get(opts, :subscribed_extensions) || [], &(&1 in @io_callbacks))
  end

  @doc """
  The jobs a plugin's channel option can name: `:in` when its provider feeds the
  relay, `:out` when the plugin acts on the records passing through it.
  Both, one, or neither.

  The two are independent and neither implies the other. A hook on
  `on_input` means "act on a payload entering the relay", never "inject
  one": an HTTP sink hooked on `on_input` sends what the relay ingested and
  feeds nothing. So feeding is a provider DECLARATION and acting on
  the chain is read from the host-injected `:subscribed_extensions`, which is
  what `io_bound?/1` answers — the `:out` half, and the one read of that list.
  """
  @spec channel_jobs(Decl.t(), map()) :: [:in | :out]
  def channel_jobs(%Decl{feeds: feeds}, opts) do
    for {job, true} <- [in: feeds, out: io_bound?(opts)], do: job
  end

  # A polling source's derived idle window, in polling intervals. Three, because
  # a poll that outlives its own interval stretches the cadence rather than
  # stacking, so a single slow cycle plus the poll after it must both land inside
  # the window, otherwise a merely slow source would read as a dead one.
  @idle_intervals 3

  @doc """
  A polling source's idle verdict: `{:unhealthy, {:poll_idle, age_ms}}` once its
  last settled poll is older than `idle_after_ms`, or than three `interval_ms`
  when that is 0.

  A failure run alone cannot see a source that has stopped polling: its run is
  zero and its last error clean, and a source delivering nothing reads exactly
  like a quiet one downstream. The idle window closes that.
  """
  @spec idle_verdict(integer(), non_neg_integer(), pos_integer()) :: health_verdict()
  def idle_verdict(last_settled_at, idle_after_ms, interval_ms) do
    window = if idle_after_ms > 0, do: idle_after_ms, else: @idle_intervals * interval_ms
    age = :erlang.monotonic_time(:millisecond) - last_settled_at

    if age > window, do: {:unhealthy, {:poll_idle, age}}, else: :healthy
  end

  @doc """
  Does this record belong to the set of channels a sink is restricted to? `nil`
  is no restriction, and any one of the names is a match.

  The ONE reading of a resolved `:filter_channel`. A provider that read the
  option rows here and re-derived their meaning would let two sinks on one
  relay disagree about which records a channel names, and would pay the
  resolution once per record instead of once per enable. The set is what the
  resolution answers however the user spelled it, so one name and a list of
  one are the same thing here and no provider can tell them apart.
  """
  @spec in_channel?(map(), [binary()] | nil) :: boolean()
  def in_channel?(_metadata, nil), do: true
  def in_channel?(metadata, channels), do: Map.get(metadata, "channel") in channels

  @doc """
  The per-plugin health-probe budget. Callers that time-box a whole relay's
  probe sweep size their allowance from this rather than from a provider's own
  call timeout.
  """
  @spec probe_budget_ms() :: pos_integer()
  def probe_budget_ms(), do: Durations.get(:provider_probe_timeout_ms)

  @fail_open_event [:flezha, :plugin, :health, :fail_open]

  @doc """
  Report `:healthy` for a probe that could not be completed, and emit the fact.

  Fail-open is deliberate — the reconciler must never escalate on a probe it
  could not finish — but it is the one verdict that cannot be trusted, and a
  provider that has stopped answering is otherwise indistinguishable from a
  healthy one.

  Called from `Extension.Table`, the only layer that reliably knows which plugin
  a provider belongs to: a provider's own opts may carry no `:plugin_id`, so an
  event emitted from inside one could not name what had gone quiet. Detection
  reports `{:fail_open, reason}` upward; this is where it becomes an event.
  """
  @spec fail_open(binary() | nil, module(), term()) :: :healthy
  def fail_open(plugin_id, provider, reason) do
    :telemetry.execute(@fail_open_event, %{count: 1}, %{
      plugin_id: plugin_id,
      provider: provider,
      reason: reason
    })

    {:fail_open, reason}
  end

  @doc """
  The ONE normaliser: run `fun` and turn whatever it answers — including a
  raise, a throw or nonsense — into a `probe_result()`. Every provider answer
  in the system passes through here, so "what shapes can a health answer take"
  is settled in one place and the judge sees one vocabulary.
  """
  @spec normalise((-> term())) :: probe_result()
  def normalise(fun) do
    case fun.() do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      {:fail_open, _reason} = uncertain -> uncertain
      other -> {:fail_open, {:invalid_return, other}}
    end
  rescue
    error -> {:fail_open, {:raised, error}}
  catch
    kind, reason -> {:fail_open, {kind, reason}}
  end

  @doc """
  The telemetry event `fail_open/3` emits, for handlers to attach to.
  """
  @spec fail_open_event() :: [atom()]
  def fail_open_event(), do: @fail_open_event

  @doc """
  The pid preamble every worker-backed `health/1` shares: resolve the tracked
  process, name its absence and its death, and otherwise delegate to the
  provider's own verdict.

  `reasons` carries `:absent` and `:dead` (the atoms this provider reports),
  optionally `:pid`, the opts keys to resolve in order — a provider that stores
  its worker under its own key lists that first and `:owned_pid` after — and
  optionally `:budget_ms` (default `probe_budget_ms/0`).

  The verdict is computed off the caller and abandoned past the budget. That
  bound is the point: `Extension.Table.health/1` runs on the convergence loop's
  own process, and providers reach their workers with a `GenServer.call` whose
  timeout is measured in seconds — so one wedged worker would otherwise cost
  every tick that much. A verdict that times out, exits, or returns nonsense
  reports healthy: the loop must never escalate on an uncertain probe, and a
  genuinely gone process is already the `:dead` branch.
  """
  @spec worker_health(map(), keyword(), (pid() -> probe_result())) :: probe_result()
  def worker_health(opts, reasons, verdict_fun),
    do: pid_health(worker_pid(opts), reasons, verdict_fun)

  @doc """
  `worker_health/3` against a pid the caller already has. The one shape a
  provider that runs MORE than one worker reads its own per-worker verdict
  through, so it never has to fabricate the host's `:owned_pid` to reuse the
  preamble.
  """
  @spec pid_health(pid() | nil, keyword(), (pid() -> probe_result())) :: probe_result()
  def pid_health(pid, reasons, verdict_fun) do
    cond do
      not is_pid(pid) ->
        {:unhealthy, Keyword.fetch!(reasons, :absent)}

      not Process.alive?(pid) ->
        {:unhealthy, Keyword.fetch!(reasons, :dead)}

      true ->
        steps_verdict(verdict_fun, pid, Keyword.get(reasons, :budget_ms, probe_budget_ms()))
    end
  end

  @doc """
  The verdict a provider that overrides `health/1` with nothing takes: the
  process the table started is alive, or there is none.

  Absent is HEALTHY, not unhealthy: a provider that declares no worker has
  nothing to be dead, and one whose worker has not been started yet is
  `:prepared`, which is not probed at all. Only a worker the table started and
  that has since died is a fault.
  """
  @spec default_health(map()) :: health_verdict()
  def default_health(opts) do
    case worker_pid(opts) do
      nil -> :healthy
      pid -> if Process.alive?(pid), do: :healthy, else: {:unhealthy, :worker_dead}
    end
  end

  @doc """
  The provider-owned process the host is tracking, resolved from `opts` in the
  order given — a provider that stores its worker under its own key lists that
  first and `:owned_pid` after. One rule, shared by `worker_health/3` and by
  any `inspect_state/1` that reaches the same process.
  """
  @spec worker_pid(map(), [atom()]) :: pid() | nil
  def worker_pid(opts, keys \\ [:owned_pid]) do
    Enum.find_value(keys, &Map.get(opts, &1))
  end

  defp steps_verdict(verdict_fun, pid, budget_ms) do
    parent = self()
    ref = make_ref()

    {child, monitor} =
      spawn_monitor(fn -> send(parent, {ref, safe_verdict(verdict_fun, pid)}) end)

    receive do
      {^ref, verdict} ->
        Process.demonitor(monitor, [:flush])
        verdict

      {:DOWN, ^monitor, :process, _pid, reason} ->
        {:fail_open, {:verdict_crashed, reason}}
    after
      budget_ms ->
        Process.exit(child, :kill)
        Process.demonitor(monitor, [:flush])
        {:fail_open, :probe_budget_exceeded}
    end
  end

  defp safe_verdict(verdict_fun, pid), do: normalise(fn -> verdict_fun.(pid) end)

  @doc """
  Ask a provider-owned worker for a health fact over a bare `{tag, from, ref}`
  message, briefly. Used where the worker is not a GenServer, or where a
  `GenServer.call` would queue behind the very backlog being diagnosed.

  The worker replies `{ref, payload}`. A worker that does not answer in time is
  reported `:timeout`, which callers read as uncertainty — see `worker_health/3`.

  `tag` is any term, so the same correlated exchange carries a request with a
  body — the shape a dispatch callback hands work to a worker it must not call
  synchronously, because the worker can be inside a call back to the caller.
  """
  @spec ask(pid(), term(), timeout()) :: {:ok, term()} | :timeout
  def ask(pid, tag, timeout \\ probe_budget_ms()) do
    ref = make_ref()
    send(pid, {tag, self(), ref})

    receive do
      {^ref, payload} -> {:ok, payload}
    after
      timeout -> :timeout
    end
  end

  @host_opts [
    :overwatcher_pid,
    :source_namespace,
    :source_relay_id,
    :source_plugin_id,
    :worker_key,
    :effectively_once,
    # Whether the relay journals its input (`Replay.floor_reporter/1`), which
    # decides how often a source moves its upstream position.
    :durable_input,
    :owned_pid,
    # Which of its provider's callbacks this plugin actually has hooks on.
    # A provider that costs nothing until it is bound needs it: the declaration
    # says what it CAN do and this says what it was asked to do.
    :subscribed_extensions,
    # The two halves the `channel`, `channel_in` and `channel_out` rows resolve
    # to, written by `Options.normalise_opts/2` and read by the injection sites
    # and `in_channel?/2`. They differ in shape because their jobs do: the entry
    # channel is ONE name, since a payload is injected on exactly one, and the
    # filter is the SET of names a record's channel may match. They are
    # host-derived rather than settable, which is what keeps the resolution a
    # pure function of the rows a user wrote: re-preparing a plugin
    # resolves them again from the same three values.
    :entry_channel,
    :filter_channel,
    # A partition group's fan-out merges these into EVERY plugin of every
    # member (`Plugin.for_partition/3` over `Partition.provider_opts/1`), so a
    # provider that does not allow them refuses every enable on a partitioned
    # relay. A provider that USES one still declares it as its own field.
    :partition_index,
    :partition_count,
    :key_group_from,
    :key_group_to
  ]

  @doc """
  The options the HOST injects into a provider's opts, never settable by
  config: the identity `Extension.Table.with_host_identity/2` merges and the
  worker key (`WorkerSup.keyed/1`) the table stores, the relay facts
  `bulk_activate/2` merges, the tracked pid `health/1` injects, and the
  partition identity `Plugin.for_partition/3` merges into a group member's
  plugins. They live in one list because every provider that calls
  `reject_unknown/3` must allow all of them — a key added to the host side and
  missed in one provider's private copy turns that provider's every enable into
  a refusal.
  """
  @spec host_opts() :: [atom()]
  def host_opts(), do: @host_opts

  @doc """
  Refuse options a provider does not recognise rather than ignoring them. The
  generated `build/1` silently drops any key that is not a struct field, which
  would let a config ask for a capability the provider deliberately does not
  offer and read as accepted.
  """
  @spec reject_unknown(Enumerable.t(), [atom() | binary()], binary()) ::
          :ok | {:error, {:unknown_options, binary(), [binary()]}}
  def reject_unknown(keys, allowed, provider) do
    case Enum.reject(keys, &(&1 in allowed)) do
      [] -> :ok
      unknown -> {:error, {:unknown_options, provider, Enum.map(unknown, &to_string/1)}}
    end
  end

  # ---------- telemetry vocabulary ----------

  @verbs [:shipped, :retried, :failed, :overflowed]

  @doc "The verbs that mean the same thing for every provider."
  @spec verbs() :: [atom()]
  def verbs(), do: @verbs

  @doc """
  Emit one provider event. The prefix is DERIVED from the provider's own
  identifier, so a provider carries no prefix attribute and cannot invent a
  root of its own — a provider under its own root is unreachable by anything
  that subscribes to the family, and the divergence is invisible until someone
  greps for it.

      emit("http_push", :shipped, %{count: 1}, labels)
      #=> [:flezha, :plugin, :http_push, :shipped]

  `shipped | retried | failed | overflowed` are the four facts every provider
  reports, declared once here so one dashboard reads them all. A fact that
  belongs to one provider alone — a broker metadata refresh, a topic creation —
  is that provider's own verb under the same derived prefix.
  """
  @spec emit(atom() | binary(), atom(), map(), map()) :: :ok
  def emit(provider, verb, measurements, metadata \\ %{}) when is_atom(verb) do
    :telemetry.execute(event(provider, verb), measurements, metadata)
  end

  @doc "The event name `emit/4` would use."
  @spec event(atom() | binary(), atom()) :: [atom()]
  def event(provider, verb) when is_binary(provider),
    do: event(String.to_existing_atom(provider), verb)

  def event(provider, verb) when is_atom(provider), do: [:flezha, :plugin, provider, verb]

  @doc """
  Declare a provider: its option rows, its worker, and the defaults for every
  callback it does not override. See
  `Flezha.Plugin.ProviderBehaviour.Declaration`, which generates all of it.
  """

  defmacro __using__(opts) do
    quote do
      use Flezha.Plugin.ProviderBehaviour.Declaration, unquote(opts)
    end
  end
end
