defmodule Flezha.Plugin.ProviderBehaviour do
  @moduledoc """
  Behaviour for plugin providers with automatic build/1 implementation.

  Usage:
    defmodule MyProvider do
      use Flezha.Plugin.ProviderBehaviour

      @enforce_keys [:required_field]
      defstruct @enforce_keys ++ [optional_field: "default"]

      # Optionally override convert_value for custom field processing
      defp convert_value(:special_field, value) do
        # custom conversion logic
      end

      # The build/1 function is automatically generated
    end
  """

  @callback plugin_type() :: :io_binding
  @callback build(map()) :: {:ok, struct()} | {:error, String.t()}
  @callback help() :: String.t()
  @callback provider_info() :: map()

  @typedoc "Health verdict for a running provider instance."
  @type health_verdict() :: :healthy | {:unhealthy, reason :: term()}

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

  @optional_callbacks health: 1, inspect_state: 1

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
  retries a failing flush: `base × 2^(attempt-1)`, capped at `max`. `attempt`
  counts from 1 (the first retry).
  """
  @spec retry_backoff_ms(pos_integer(), pos_integer(), pos_integer()) :: pos_integer()
  def retry_backoff_ms(base, max, attempt) when attempt >= 1 do
    min(base * Integer.pow(2, attempt - 1), max)
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
    if not saturated?(state) do
      Enum.each(state.waiters, &GenServer.reply(&1, :ok))
      %{state | waiters: []}
    else
      state
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
  @spec worker_health(map(), keyword(), (pid() -> health_verdict())) :: health_verdict()
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

      {:DOWN, ^monitor, :process, _pid, _reason} ->
        :healthy
    after
      budget_ms ->
        Process.exit(child, :kill)
        Process.demonitor(monitor, [:flush])
        :healthy
    end
  end

  defp safe_verdict(verdict_fun, pid) do
    case verdict_fun.(pid) do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      _ -> :healthy
    end
  catch
    _, _ -> :healthy
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

  defmacro __using__(_opts) do
    quote do
      @behaviour Flezha.Plugin.ProviderBehaviour
      @before_compile Flezha.Plugin.ProviderBehaviour

      def build(string_map) do
        # Try to determine required fields by attempting to create struct
        enforce_keys = get_required_fields(__MODULE__)

        # Get struct fields at runtime - use a sample with dummy values for enforce_keys
        dummy_params =
          Enum.reduce(enforce_keys, %{}, fn key, acc ->
            Map.put(acc, key, "dummy")
          end)

        struct_sample = struct!(__MODULE__, dummy_params)
        struct_fields = Map.keys(struct_sample) |> Enum.reject(&(&1 == :__struct__))

        {convert_status, params_data} =
          Enum.reduce_while(struct_fields, {:ok, %{}}, fn field, {_status, params} ->
            field_str = Atom.to_string(field)

            case Map.get(string_map, field_str) do
              nil ->
                {:cont, {:ok, params}}

              value ->
                case convert_value(field, value) do
                  {:ok, converted_value} ->
                    params = Map.put(params, field, converted_value)
                    {:cont, {:ok, params}}

                  {:error, reason} ->
                    {:halt, {:error, "Invalid value for #{field}: #{reason}"}}
                end
            end
          end)

        # Validate required fields
        missing_fields =
          Enum.filter(enforce_keys, fn key ->
            not Map.has_key?(params_data, key)
          end)

        cond do
          !Enum.empty?(missing_fields) ->
            {:error, "Missing required fields: #{inspect(missing_fields)}"}

          convert_status == :error ->
            {:error, "Failed to convert fields: #{inspect(params_data)}"}

          true ->
            try do
              {:ok, struct!(__MODULE__, params_data)}
            rescue
              e -> {:error, "Failed to create struct: #{Exception.message(e)}"}
            end
        end
      end

      def help() do
        module_name = __MODULE__ |> Module.split() |> List.last()

        # Get struct fields
        enforce_keys = get_required_fields(__MODULE__)

        dummy_params =
          Enum.reduce(enforce_keys, %{}, fn key, acc -> Map.put(acc, key, "dummy") end)

        struct_sample = struct!(__MODULE__, dummy_params)
        struct_fields = Map.keys(struct_sample) |> Enum.reject(&(&1 == :__struct__))

        required_fields = Enum.map(enforce_keys, &Atom.to_string/1)

        optional_fields =
          struct_fields |> Enum.reject(&(&1 in enforce_keys)) |> Enum.map(&Atom.to_string/1)

        help_text =
          """
          #{module_name} Plugin Help

          USAGE:
              flz plugin enable <plugin_id> #{module_name |> String.downcase()} [OPTIONS]

          REQUIRED PARAMETERS (X-prefixed):
          """ <>
            if Enum.empty?(required_fields) do
              "    None\n"
            else
              (Enum.map(required_fields, fn field ->
                 "    --X#{field} <value>    #{String.capitalize(field)}"
               end)
               |> Enum.join("\n")) <> "\n"
            end <>
            """

            OPTIONAL PARAMETERS (X-prefixed):
            """ <>
            if Enum.empty?(optional_fields) do
              "    None\n"
            else
              (Enum.map(optional_fields, fn field ->
                 "    --X#{field} <value>    #{String.capitalize(field)}"
               end)
               |> Enum.join("\n")) <> "\n"
            end <>
            """

            COMMON OPTIONS:
                --namespace <namespace>    Namespace (default: default)
                --operative_id <id>       Target operative ID
                --trigger <trigger>       Trigger mode (on_input|on_output|proactive|interval)
                --position <num>          Plugin position

            Override this help/0 function in your provider module for custom help text.
            """

        help_text
      end

      def provider_info() do
        enforce_keys = get_required_fields(__MODULE__)

        dummy_params =
          Enum.reduce(enforce_keys, %{}, fn key, acc -> Map.put(acc, key, "dummy") end)

        struct_sample = struct!(__MODULE__, dummy_params)
        all_fields = Map.keys(struct_sample) |> Enum.reject(&(&1 == :__struct__))
        optional_fields = all_fields -- enforce_keys

        %{
          module: inspect(__MODULE__),
          type: plugin_type(),
          required_fields: Enum.map(enforce_keys, &Atom.to_string/1),
          optional_fields: Enum.map(optional_fields, &Atom.to_string/1)
        }
      end

      defoverridable help: 0, provider_info: 0

      defp get_required_fields(module) do
        try do
          # Try to create struct with empty map - if it succeeds, no required fields
          struct!(module, %{})
          []
        rescue
          e in ArgumentError ->
            # Parse the error message to extract required field names
            message = Exception.message(e)

            case Regex.run(
                   ~r/the following keys must also be given when building struct (.+): \[(.+)\]/,
                   message
                 ) do
              [_, _struct_name, fields_str] ->
                fields_str
                |> String.split(", ")
                |> Enum.map(&String.trim/1)
                |> Enum.map(fn field_str ->
                  # Remove the colon prefix if present
                  field_str = String.replace_prefix(field_str, ":", "")
                  String.to_existing_atom(field_str)
                end)

              _ ->
                []
            end
        end
      end
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
