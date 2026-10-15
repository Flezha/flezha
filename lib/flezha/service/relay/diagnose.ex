defmodule Flezha.Service.Relay.Diagnose do
  @moduledoc """
  What `Flezha.Service.Relay.diagnose/3` answers: one relay's verdict and the
  findings that explain it.

  It reads only what the controller already keeps: the relay's Overwatch state
  as `Flezha.Service.Relay.Info` reads it, the convergence loop's verdict records
  for the relay, its runner and its plugins, the runners that announce its
  factory, its dead letters, its input log and, for a finished relay, its
  completion record.
  It collects and stores nothing, so a fact nobody records is not reported.

  Every finding is a code of the error catalogue's `diagnose` family, so its
  message and hint are worded there, once.
  """

  alias Flezha.Convergence.Snapshot
  alias Flezha.Convergence.Verdict
  alias Flezha.Dlq.Store
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Relay.Info
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Admission
  alias Flezha.State.InputLog
  alias Flezha.State.InputLog.Sweep

  # Every finding with its level, problems first: findings are listed in this
  # order, so the first one's level is the diagnosis's verdict.
  @codes [
    no_runner: :problem,
    runner_mismatch: :problem,
    runner_disconnected: :problem,
    unbound: :problem,
    data_plane_wedge: :problem,
    output_stall: :problem,
    stuck: :problem,
    plugin_missing: :problem,
    plugin_unhealthy: :problem,
    parked: :warning,
    dead_letters: :warning,
    inputs_stuck: :warning,
    healthy: :note,
    rebinding: :note,
    asleep: :note,
    finished: :note,
    inputs_waiting: :note,
    plugin_disabled: :note,
    unknown: :note
  ]

  # The finding a relay verdict's reason names, by the reason's tag. The two
  # binding findings are refined by the runners the factory has.
  @reasons %{
    unobserved: :unknown,
    evicted_at_boundary: :asleep,
    no_binding: :unbound,
    dead_runner: :runner_disconnected,
    parked: :parked,
    wedged: :data_plane_wedge,
    stalled: :output_stall,
    stuck: :stuck,
    plugin_unhealthy: :plugin_unhealthy,
    producing: :healthy
  }

  @error_chars 120

  @type finding :: %{
          level: :problem | :warning | :note,
          code: String.t(),
          message: String.t(),
          details: map(),
          hint: String.t() | nil
        }

  @doc """
  The diagnosis of `relay_id` in `namespace`: `verdict` is the highest level
  among the findings (`ok` when there are only notes), and the findings are
  ordered by level, then by kind. A relay neither live nor retained as
  finished answers `relay_not_found`.

  `errors?` says whether the caller may read dead letters, and so their error
  texts; without it the dead-letter finding carries the count alone.
  """
  @spec report(String.t(), String.t(), boolean()) ::
          {:ok, %{relay: String.t(), verdict: :ok | :warning | :problem, findings: [finding()]}}
          | {:error, term()}
  def report(namespace, relay_id, errors?) do
    with {:ok, found} <- found(namespace, relay_id, errors?) do
      findings =
        found
        |> Enum.uniq_by(fn {code, details} -> {code, details[:plugin]} end)
        |> Enum.sort_by(fn {code, _details} ->
          Enum.find_index(@codes, &(elem(&1, 0) == code))
        end)
        |> Enum.map(&render(&1, %{relay: relay_id, namespace: namespace}))

      {:ok, %{relay: relay_id, verdict: verdict(findings), findings: findings}}
    end
  end

  @doc "The finding a relay verdict's `reason` names; `nil` for one no finding maps."
  @spec reason_code(term()) :: atom() | nil
  def reason_code(reason) when is_tuple(reason), do: Map.get(@reasons, elem(reason, 0))
  def reason_code(reason), do: Map.get(@reasons, reason)

  @doc "Every finding code, in order."
  @spec codes() :: [String.t()]
  def codes(), do: Enum.map(@codes, fn {code, _level} -> "diagnose.#{code}" end)

  defp found(namespace, relay_id, errors?) do
    case Info.relay(namespace, relay_id) do
      {:live, state} ->
        {:ok, live(namespace, relay_id, state, errors?)}

      {:finished, row} ->
        {:ok, [{:finished, Map.take(row, [:status, :error])} | dead_letters(relay_id, errors?)]}

      {:error, _reason} = refused ->
        refused
    end
  end

  # The dead letters and the input log are files of the controller the relay
  # runs on, so they are read only when that is this one.
  defp live(namespace, relay_id, state, errors?) do
    class = Info.class_of(relay_id, state)
    node = relay_node(namespace, relay_id)
    local = if node == node(), do: dead_letters(class, errors?) ++ inputs(class), else: []
    others = runner(state.runner_id) ++ plugins(namespace, relay_id, state, node) ++ local

    case verdict_finding(namespace, relay_id, state) do
      {:healthy, details} ->
        if Enum.all?(others, &(level(&1) == :note)),
          do: [{:healthy, Map.put(details, :node, node)} | others],
          else: others

      finding ->
        [finding | others]
    end
  end

  # ---------- the verdict ----------

  defp verdict_finding(namespace, relay_id, state) do
    case Snapshot.verdict(namespace, relay_id) do
      %Verdict{reason: reason} = verdict ->
        reason
        |> reason_code()
        |> binding(namespace, verdict, state)
        |> verdict_details(verdict, state)

      nil ->
        {:unknown, %{}}
    end
  end

  # An unbound or orphaned relay is first a question of who could take it.
  defp binding(code, namespace, verdict, state) when code in [:unbound, :runner_disconnected] do
    factory = state |> Map.get(:builder, %{}) |> Map.get(:factory_id)

    case factory && FleetManager.placement_refusal(namespace, factory) do
      :no_such_factory ->
        {:no_runner, %{factory: factory}}

      refusal when refusal in [:none, nil] ->
        if Verdict.listed_state(verdict.state, Snapshot.unmet_intent()) == :recovering,
          do: :rebinding,
          else: code

      refusal ->
        reason = Admission.placement_reason(refusal, namespace, factory)
        {:runner_mismatch, %{factory: factory, reason: text(reason)}}
    end
  end

  defp binding(code, _namespace, _verdict, _state), do: code

  defp verdict_details({code, details}, _verdict, _state), do: {code, details}

  defp verdict_details(code, %Verdict{reason: reason} = verdict, state) do
    seconds = seconds_since(verdict.since_ms)

    details =
      case {code, reason} do
        {:runner_disconnected, _} -> %{runner: state.runner_id, seconds: seconds}
        {:parked, {:parked, why}} -> %{seconds: seconds, reason: text(why)}
        {:data_plane_wedge, {:wedged, layer}} -> %{layer: layer, seconds: seconds}
        {:stuck, {:stuck, detail}} -> %{detail: detail, seconds: seconds}
        {:plugin_unhealthy, {_, plugin, why}} -> plugin_details(state, plugin, why)
        {:healthy, _} -> %{runner: state.runner_id}
        _seconds_only -> %{seconds: seconds}
      end

    {code, details}
  end

  # ---------- the other facts ----------

  # A runner the loop judged wedged stops every relay bound to it, while each
  # of those relays' own records may still read healthy.
  defp runner(runner_id) do
    case Snapshot.record({:runner, runner_id}) do
      %Verdict{reason: {:wedged, layer}, since_ms: since} ->
        [{:data_plane_wedge, %{layer: layer, seconds: seconds_since(since)}}]

      _none ->
        []
    end
  end

  # A plugin whose provider this controller lacks, read only where the relay
  # runs here, every plugin whose own verdict record is degraded, and every
  # plugin its operator disabled.
  defp plugins(namespace, relay_id, state, node) do
    for {plugin_id, plugin} <- bound(state),
        finding = plugin_finding(namespace, relay_id, state, {plugin_id, plugin.provider}, node),
        do: finding
  end

  defp bound(state), do: state |> Map.get(:extensions, %{}) |> Map.get(:plugins, %{})

  defp plugin_finding(namespace, relay_id, state, {plugin_id, provider}, node) do
    record = Snapshot.record({:plugin, {namespace, relay_id, plugin_id}})

    cond do
      node == node() and not Code.ensure_loaded?(provider) ->
        {:plugin_missing, %{plugin: plugin_id, provider: inspect(provider), node: node}}

      match?(%Verdict{state: :degraded}, record) ->
        {:plugin_unhealthy, plugin_details(state, plugin_id, record.reason)}

      match?(%Verdict{reason: :disabled}, record) ->
        {:plugin_disabled, %{plugin: plugin_id}}

      true ->
        nil
    end
  end

  defp dead_letters(class, errors?) do
    case Store.list(class) do
      [] ->
        []

      entries ->
        newest = entries |> Enum.map(&(&1["ts_ms"] || 0)) |> Enum.max()

        details = %{
          count: length(entries),
          newest_s: div(System.system_time(:millisecond) - newest, 1000),
          class: class
        }

        [
          {:dead_letters,
           if(errors?, do: Map.put(details, :errors, errors(entries)), else: details)}
        ]
    end
  end

  # The three most common error texts, which only a caller who may read the
  # dead letters is shown.
  defp errors(entries) do
    entries
    |> Enum.frequencies_by(&String.slice(&1["error"] || "", 0, @error_chars))
    |> Enum.sort_by(fn {error, count} -> {-count, error} end)
    |> Enum.take(3)
    |> Enum.map(&elem(&1, 0))
  end

  # Inputs above the floor, told apart by the frozen-watermark condition the
  # input log's sweep emits its event under.
  defp inputs(class) do
    case InputLog.tail(class) do
      {:ok, floor, last} when last > floor ->
        count = last - floor

        case Sweep.frozen_size(class) do
          nil -> [{:inputs_waiting, %{count: count}}]
          size -> [{:inputs_stuck, %{count: count, size_bytes: size}}]
        end

      _none_or_refused ->
        []
    end
  end

  # ---------- shaping ----------

  defp relay_node(namespace, relay_id) do
    case RelayManager.get_relay_pid(namespace, relay_id) do
      {:ok, pid} -> node(pid)
      _unresolved -> nil
    end
  end

  defp render({code, details}, base) do
    error = Flezha.Error.new("diagnose.#{code}", Map.merge(base, details))

    %{
      level: level({code, details}),
      code: error.code,
      message: error.message,
      details: error.details,
      hint: error.hint
    }
  end

  defp level({code, _details}), do: Keyword.fetch!(@codes, code)

  defp verdict([%{level: level} | _]) when level != :note, do: level
  defp verdict(_notes), do: :ok

  defp seconds_since(since_ms) when is_integer(since_ms),
    do: div(max(0, System.monotonic_time(:millisecond) - since_ms), 1000)

  defp seconds_since(_unknown), do: 0

  defp plugin_details(state, plugin_id, why) do
    provider = state |> bound() |> Map.get(plugin_id, %{}) |> Map.get(:provider)
    %{plugin: plugin_id, provider: inspect(provider), reason: plugin_reason(why)}
  end

  # What a plugin's own probe said, without the wrapper the judge puts on it.
  defp plugin_reason({tag, why}) when tag in [:unhealthy, :plugin_unhealthy], do: text(why)
  defp plugin_reason(why), do: text(why)

  # A reason in the catalogue's own words when it has some, else as written.
  defp text(reason) when is_binary(reason), do: reason

  defp text(reason) do
    if Flezha.Error.catalogued?(reason, nil),
      do: Flezha.Error.from_reason(reason, nil).message,
      else: inspect(reason)
  end
end
