defmodule Flezha.Convergence.Probe do
  @moduledoc """
  Pure observer of live, ephemeral reality — "what the system actually
  looks like right now". No decisions, no side effects.

  `snapshot/0` builds a `Reality` map the Loop diffs against
  `Intent.declared/0`.
  """

  alias Flezha.Convergence.{SafeCall, Snapshot}
  alias Flezha.Executor.FleetManager
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.HubManager

  @relay_call_timeout_ms 1_000
  # Headroom above the Overwatch call for provider health probes (the
  # websocket liveness probe blocks up to 500 ms per wedged connection).
  @health_probe_budget_ms 1_500

  @type reality :: %{
          relays: %{{binary(), binary()} => map()},
          active_executors: %{binary() => MapSet.t() | :unknown},
          recovery: %{atom() => atom()},
          tick_at: integer()
        }

  # ---------- reality ----------

  @spec snapshot() :: reality()
  def snapshot() do
    namespaces = running_namespaces()

    %{
      relays: relays(namespaces),
      active_executors: active_executors(namespaces),
      recovery: Snapshot.recovery_states(),
      tick_at: System.monotonic_time(:millisecond)
    }
  end

  # Probe every relay concurrently: one wedged Overwatch costs ~one timeout,
  # not N×timeout serial in the Loop process. A relay we cannot read is
  # recorded as `bound_executor: :unknown` so the Diff skips it — a failed
  # observation must never drive a mutating heal.
  defp relays(namespaces) do
    pairs =
      Enum.flat_map(namespaces, fn ns ->
        Enum.map(safe_relays(ns), fn {relay_id, pid} -> {{ns, relay_id}, pid} end)
      end)

    pairs
    |> Task.async_stream(
      fn {ref, pid} -> {ref, observe_relay(pid)} end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: @relay_call_timeout_ms + @health_probe_budget_ms,
      on_timeout: :kill_task,
      ordered: true
    )
    |> Enum.zip(pairs)
    |> Enum.reduce(%{}, fn
      {{:ok, {ref, observed}}, _pair}, acc -> Map.put(acc, ref, observed)
      {{:exit, _}, {ref, _pid}}, acc -> Map.put(acc, ref, unreadable())
    end)
  end

  defp observe_relay(pid) when is_pid(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, @relay_call_timeout_ms) do
      {:ok, ow} when is_map(ow) ->
        %{
          pid: pid,
          bound_executor: Map.get(ow, :executor_id),
          factory_id: Map.get(ow, :factory_id),
          parked?: Map.get(ow, :init_error) != nil,
          running_plugins: running_plugins(ow),
          plugin_health: plugin_health(pid, ow)
        }

      _ ->
        unreadable()
    end
  end

  defp observe_relay(_),
    do: %{
      pid: nil,
      bound_executor: nil,
      factory_id: nil,
      parked?: false,
      running_plugins: [],
      plugin_health: %{}
    }

  defp unreadable(),
    do: %{
      pid: nil,
      bound_executor: :unknown,
      factory_id: nil,
      parked?: false,
      running_plugins: [],
      plugin_health: %{}
    }

  # The plugin ids currently subscribed in the relay's extension table.
  defp running_plugins(ow) do
    case Map.get(ow, :extensions) do
      %{plugins: plugins} when is_map(plugins) -> Map.keys(plugins)
      _ -> []
    end
  end

  # Provider self-reported health (`Extension.Table.health/1`). Owned plugin
  # pids live on the Overwatch's node, so only the loop running there can
  # probe them; elsewhere — and on any failed read — report `%{}`: an
  # uncertain probe is healthy and must never drive a heal.
  defp plugin_health(pid, ow) do
    with true <- node(pid) == node(),
         %Table{} = table <- Map.get(ow, :extensions) do
      Table.health(table)
    else
      _ -> %{}
    end
  rescue
    _ -> %{}
  catch
    _, _ -> %{}
  end

  # On a failed FleetManager read we record `:unknown`, NOT an empty set:
  # the Diff must skip misbound classification for the namespace rather than
  # mass-migrate every relay on a transient read failure.
  defp active_executors(namespaces) do
    Enum.into(namespaces, %{}, fn ns ->
      executors =
        try do
          FleetManager.active_executors(ns) |> MapSet.new()
        catch
          :exit, _ -> :unknown
        end

      {ns, executors}
    end)
  end

  # ---------- helpers ----------

  defp running_namespaces() do
    HubManager.get_running_namespaces()
  catch
    :exit, _ -> []
  end

  defp safe_relays(namespace) do
    HubManager.get_all_relays(namespace)
  catch
    :exit, _ -> []
  end
end
