defmodule Flezha.Plugin.ProviderBehaviour do
  @moduledoc """
  The plugin provider contract, and the shared helpers every provider uses to
  honour it.

  `use ProviderBehaviour` generates `build/1`, `help/0` and `provider_info/0`
  from the module's own `defstruct`: `@enforce_keys` are the required options
  and the rest are optional, both in declaration order. A provider supplies
  only its private `convert_value/2` coercion table, which `build/1` runs over
  a CLI string map — and only over that path, never over typed opts.

  Declaring `@known_opts` makes the provider strict: `__before_compile__` then
  generates both halves of the refusal, `build/1` for the CLI path and
  `validate_opts/1` for the `init_provider/1` one.
  """

  @callback plugin_type() :: :io_binding
  @callback build(map()) :: {:ok, struct()} | {:error, String.t()}
  @callback help() :: String.t()
  @callback provider_info() :: map()

  @typedoc "Health verdict for a running provider instance."
  @type health_verdict() :: :healthy | {:unhealthy, reason :: term()}

  @typedoc """
  A verdict plus the third answer a probe can give: `{:fail_open, reason}` for a
  probe that could not be completed. It reads as healthy — the reconciler must
  never escalate on uncertainty — but it is NOT a verdict, and the layer that
  knows the plugin's identity turns it into telemetry (`Extension.Table`).
  """
  @type probe_result() :: health_verdict() | {:fail_open, reason :: term()}

  @doc """
  Optional health verdict for a *running* provider instance. The reconciler
  (`Flezha.Plugin.Extension.Table.health/1`) queries this to escalate — restart
  the provider under a bounded governor — only after the provider's own fast
  in-process recovery has repeatedly failed. `opts` is the provider's stored
  opts with `:owned_pid` (its tracked process, if any) injected by the host.

  Default when a provider does NOT export `health/1`: the owned pid being alive
  (or absent) is healthy. Providers SHOULD reuse their existing liveness state
  rather than introduce new timers (e.g. the WebSocket provider reuses
  `last_frame_at`/`last_data_frame_at`).
  """
  @callback health(opts :: map()) :: health_verdict()

  @doc """
  Optional operator-facing detail for a *running* provider instance, rendered
  by `flz operative info` beneath the plugin that reported it. `opts` is the
  provider's stored opts with `:owned_pid` injected, exactly as for `health/1`.

  This is for state an operator must be able to see and the plugin's static
  configuration does not show — the websocket provider's applied subscription
  set, the outcome of its last reconcile, its last error. Return `nil` when
  there is nothing to add, so a provider can export it and still stay silent
  for the instances it does not apply to.
  """
  @callback inspect_state(opts :: map()) :: map() | nil

  @doc """
  Optional teardown for a provider that owns more processes than the single
  `:owned_pid` the host tracks — the one hook a fan-out provider has to release
  what the host cannot see.

  Called by `Flezha.Plugin.Extension.Table` on both exits: a plugin disable
  (`do_unsubscribe`) and an operative teardown (`teardown_all`). It must be
  idempotent, since a disable followed by a stop calls it twice.
  """
  @callback terminate_provider(opts :: map()) :: :ok

  @optional_callbacks health: 1, inspect_state: 1, terminate_provider: 1

  @doc """
  Shared `convert_value/2` result helpers: CLI plugin opts arrive as
  strings, direct (test/Elixir) opts as native types — accept both.
  """
  def parse_int(value, _field) when is_integer(value), do: {:ok, value}

  def parse_int(value, field) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> {:ok, int_val}
      _ -> {:error, "Invalid value for #{field}: #{value}"}
    end
  end

  def parse_bool(value, _field) when is_boolean(value), do: {:ok, value}
  def parse_bool("true", _field), do: {:ok, true}
  def parse_bool("false", _field), do: {:ok, false}
  def parse_bool(value, field), do: {:error, "Invalid value for #{field}: #{inspect(value)}"}

  @doc """
  The sink retry ladder shared by every provider that holds a bounded queue and
  retries a failing flush: `base × 2^(attempt-1) + jitter_ms`, capped at `max`.
  `attempt` counts from 1 (the first retry). A caller that spreads a thundering
  herd passes its own jitter; the default is the plain ladder.
  """
  @spec retry_backoff_ms(pos_integer(), pos_integer(), pos_integer(), non_neg_integer()) ::
          pos_integer()
  def retry_backoff_ms(base, max, attempt, jitter_ms \\ 0) when attempt >= 1 do
    min(base * Integer.pow(2, attempt - 1) + jitter_ms, max)
  end

  @doc "Has the worker's queue reached its bound?"
  @spec saturated?(struct()) :: boolean()
  def saturated?(state), do: state.buffer_len >= state.queue_max

  @doc """
  Reply to an enqueueing caller, or park it — the "delay, never loss" overflow
  policy shared by the sink workers that bound their queue and back-pressure
  instead of dropping. Pairs with `release_waiters/1`; the worker state must
  carry `:buffer_len`, `:queue_max`, and `:waiters`.
  """
  @spec park_or_reply(struct(), GenServer.from()) ::
          {:reply, :ok, struct()} | {:noreply, struct()}
  def park_or_reply(state, from) do
    if saturated?(state) do
      {:noreply, %{state | waiters: [from | state.waiters]}}
    else
      {:reply, :ok, state}
    end
  end

  @doc """
  Release every parked enqueue caller once the queue has drained back under
  `:queue_max`. Counterpart to `park_or_reply/2`.
  """
  @spec release_waiters(struct()) :: struct()
  def release_waiters(%{waiters: []} = state), do: state

  def release_waiters(state) do
    if saturated?(state) do
      state
    else
      Enum.each(state.waiters, &GenServer.reply(&1, :ok))
      %{state | waiters: []}
    end
  end

  @doc "Raising variants for `init_provider/1`, where opts are pre-validated."
  def coerce_int(v) when is_integer(v), do: v
  def coerce_int(v) when is_binary(v), do: String.to_integer(v)

  def coerce_bool(v) when is_boolean(v), do: v
  def coerce_bool("true"), do: true
  def coerce_bool("false"), do: false

  @probe_timeout_ms 500

  @doc """
  The per-plugin health-probe budget. Callers that time-box a whole operative's
  probe sweep size their allowance from this rather than from a provider's own
  call timeout.
  """
  @spec probe_budget_ms() :: pos_integer()
  def probe_budget_ms(), do: @probe_timeout_ms

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

    :healthy
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
  optionally `:budget_ms` (default #{@probe_timeout_ms}).

  The verdict is computed off the caller and abandoned past the budget. That
  bound is the point: `Extension.Table.health/1` runs on the convergence loop's
  own process, and providers reach their workers with a `GenServer.call` whose
  timeout is measured in seconds — so one wedged worker would otherwise cost
  every tick that much. A verdict that times out, exits, or returns nonsense
  reports healthy: the loop must never escalate on an uncertain probe, and a
  genuinely gone process is already the `:dead` branch.
  """
  @spec worker_health(map(), keyword(), (pid() -> probe_result())) :: probe_result()
  def worker_health(opts, reasons, verdict_fun) do
    pid = worker_pid(opts, Keyword.get(reasons, :pid, [:owned_pid]))

    cond do
      not is_pid(pid) ->
        {:unhealthy, Keyword.fetch!(reasons, :absent)}

      not Process.alive?(pid) ->
        {:unhealthy, Keyword.fetch!(reasons, :dead)}

      true ->
        workflow_verdict(verdict_fun, pid, Keyword.get(reasons, :budget_ms, @probe_timeout_ms))
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

  defp workflow_verdict(verdict_fun, pid, budget_ms) do
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

  defp safe_verdict(verdict_fun, pid) do
    case verdict_fun.(pid) do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      {:fail_open, _reason} = uncertain -> uncertain
      other -> {:fail_open, {:invalid_return, other}}
    end
  catch
    kind, reason -> {:fail_open, {kind, reason}}
  end

  @doc """
  Ask a provider-owned worker for a health fact over a bare `{tag, from, ref}`
  message, briefly. Used where the worker is not a GenServer, or where a
  `GenServer.call` would queue behind the very backlog being diagnosed.

  The worker replies `{ref, payload}`. A worker that does not answer in time is
  reported `:timeout`, which callers read as uncertainty — see `worker_health/3`.
  """
  @spec ask(pid(), atom(), timeout()) :: {:ok, term()} | :timeout
  def ask(pid, tag, timeout \\ @probe_timeout_ms) do
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
    :source_operative_id,
    :source_plugin_id,
    :effectively_once,
    :owned_pid
  ]

  @doc """
  The options the HOST injects into a provider's opts, never settable by
  config: the identity `Extension.Table.with_host_identity/2` merges, the
  operative facts `bulk_activate/2` merges, and the tracked pid `health/1`
  injects.

  They live in one list because every provider that calls `reject_unknown/3`
  must allow all of them — a key added to the host side and missed in one
  provider's private copy turns that provider's every enable into a refusal.
  """
  @spec host_opts() :: [atom()]
  def host_opts(), do: @host_opts

  @doc """
  Refuse options a provider does not recognise rather than ignoring them. The
  generated `build/1` silently drops any key that is not a struct field, which
  would let a config ask for a capability the provider deliberately does not
  offer and read as accepted.
  """
  @spec reject_unknown(Enumerable.t(), [atom() | binary()], binary()) :: :ok | {:error, binary()}
  def reject_unknown(keys, allowed, provider) do
    case Enum.reject(keys, &(&1 in allowed)) do
      [] ->
        :ok

      unknown ->
        {:error,
         "unknown option(s) for #{provider}: #{Enum.map_join(unknown, ", ", &to_string/1)}"}
    end
  end

  @doc """
  `module`'s struct field names as `{required, all}`, both in `defstruct`
  declaration order; `required` is its `@enforce_keys` and the optional set is
  `all -- required`.

  The order comes from `__info__/1` and not from the struct's own keys because
  `Map.keys/1` on a struct answers in VM atom-table order — which varies with
  what else the runtime has loaded — and this list is rendered into an
  operator-facing help screen and into the order `build_struct/3` reports the
  first unconvertible value in.
  """
  @spec fields(module()) :: {[atom()], [atom()]}
  def fields(module) do
    case module.__info__(:struct) do
      nil ->
        raise ArgumentError, "#{inspect(module)} uses ProviderBehaviour but declares no struct"

      declared ->
        {for(%{field: name, required: true} <- declared, do: name),
         Enum.map(declared, & &1.field)}
    end
  end

  @doc """
  Build `module`'s struct from a CLI string map, coercing each field through
  the provider's own `convert`. Answers the first value `convert` refuses.
  """
  @spec build_struct(module(), map(), (atom(), term() -> {:ok, term()} | {:error, binary()})) ::
          {:ok, struct()} | {:error, binary()}
  def build_struct(module, string_map, convert) do
    {required, all} = fields(module)

    # A halted reduce answers the refusal, never the accumulated params, so the
    # required-field check runs only on the map the success arm built. `convert`
    # has already formatted the reason; wrapping it again names the field twice.
    case Enum.reduce_while(all, %{}, &coerce_field(&1, &2, string_map, convert)) do
      {:error, reason} ->
        {:error, reason}

      params ->
        case Enum.reject(required, &Map.has_key?(params, &1)) do
          [] -> new_struct(module, params)
          missing -> {:error, "Missing required fields: #{inspect(missing)}"}
        end
    end
  end

  defp coerce_field(field, params, string_map, convert) do
    case Map.get(string_map, Atom.to_string(field)) do
      nil -> {:cont, params}
      value -> put_converted(params, field, convert.(field, value))
    end
  end

  defp put_converted(params, field, {:ok, converted}),
    do: {:cont, Map.put(params, field, converted)}

  defp put_converted(_params, _field, {:error, reason}), do: {:halt, {:error, reason}}

  defp new_struct(module, params) do
    {:ok, struct!(module, params)}
  rescue
    e -> {:error, "Failed to create struct: #{Exception.message(e)}"}
  end

  @doc "The screen `flz plugin help <provider>` renders for `module`."
  @spec render_help(module()) :: binary()
  def render_help(module) do
    {required, all} = fields(module)
    name = module |> Module.split() |> List.last()

    """
    #{name} Plugin Help

    USAGE:
        flz plugin enable <plugin_id> #{String.downcase(name)} [OPTIONS]
    """ <>
      param_block("REQUIRED", required) <>
      param_block("OPTIONAL", all -- required) <>
      """

      COMMON OPTIONS:
          --namespace <namespace>    Namespace (default: default)
          --operative_id <id>       Target operative ID
          --trigger <trigger>       Trigger mode (on_input|on_output|proactive|interval)
          --position <num>          Plugin position

      Override this help/0 function in your provider module for custom help text.
      """
  end

  # The REQUIRED and OPTIONAL sections are one rendering over two lists.
  defp param_block(heading, fields) do
    "\n#{heading} PARAMETERS (X-prefixed):\n" <> param_lines(fields)
  end

  defp param_lines([]), do: "    None\n"

  defp param_lines(fields) do
    Enum.map_join(fields, "", fn field ->
      name = Atom.to_string(field)
      "    --X#{name} <value>    #{String.capitalize(name)}\n"
    end)
  end

  @doc "`module`'s shape, as `flz plugin list` reports it."
  @spec describe(module()) :: map()
  def describe(module) do
    {required, all} = fields(module)

    %{
      module: inspect(module),
      type: module.plugin_type(),
      required_fields: Enum.map(required, &Atom.to_string/1),
      optional_fields: Enum.map(all -- required, &Atom.to_string/1)
    }
  end

  defmacro __using__(_opts) do
    behaviour = __MODULE__

    quote do
      @behaviour unquote(behaviour)
      @before_compile unquote(behaviour)

      # `convert_value/2` is the provider's own private coercion table; a
      # capture expands in its context, so the shared builder can call it.
      def build(string_map),
        do: unquote(behaviour).build_struct(__MODULE__, string_map, &convert_value/2)

      def help(), do: unquote(behaviour).render_help(__MODULE__)

      def provider_info(), do: unquote(behaviour).describe(__MODULE__)

      defoverridable help: 0, provider_info: 0
    end
  end

  defmacro __before_compile__(env) do
    case strict_opts(env) do
      nil -> catch_all()
      strict -> [strict, catch_all()]
    end
  end

  # A provider that declares `@known_opts` gets BOTH halves of the strict-option
  # contract generated: the `build/1` override (the CLI struct path) and
  # `validate_opts/1` (the `init_provider/1` path). Both are required and for
  # different reasons — `Plugin.SpecDecoder` hands a `--plugins` spec straight to
  # `init_provider/1` without ever constructing the struct, so a check living
  # only in `build/1` never runs on the path an operator actually uses.
  #
  # `@internal_opts` are accepted by `init_provider/1` but never offered to
  # `build/1`: provider-committed state read back from the cspec, not config.
  defp strict_opts(env) do
    case Module.get_attribute(env.module, :known_opts) do
      nil ->
        nil

      known ->
        internal = Module.get_attribute(env.module, :internal_opts) || []
        name = env.module |> Module.split() |> List.last() |> Macro.underscore()
        strings = Enum.map(known, &to_string/1)

        behaviour = __MODULE__

        quote do
          defoverridable build: 1

          @doc """
          Reject options this provider does not recognise rather than ignoring
          them. The generated `build/1` silently drops any key that is not a
          struct field, which would let a config ask for a capability the
          provider deliberately does not offer and read as accepted.
          """
          def build(string_map) when is_map(string_map) do
            case unquote(behaviour).reject_unknown(
                   Map.keys(string_map),
                   unquote(strings),
                   unquote(name)
                 ) do
              :ok -> super(string_map)
              err -> err
            end
          end

          @doc false
          @spec validate_opts(map()) :: :ok | {:error, binary()}
          def validate_opts(opts) do
            allowed =
              unquote(known) ++ unquote(internal) ++ unquote(behaviour).host_opts()

            unquote(behaviour).reject_unknown(Map.keys(opts), allowed, unquote(name))
          end
        end
    end
  end

  defp catch_all do
    quote do
      # Default convert_value catch-all clause - injected at the end
      defp convert_value(nil, value), do: {:error, "Empty value provided for field"}
      defp convert_value(_field, value), do: {:ok, value}
    end
  end
end
