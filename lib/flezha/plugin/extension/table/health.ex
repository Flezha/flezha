defmodule Flezha.Plugin.Extension.Table.Health do
  @moduledoc """
  The extension table's health verdicts, and the monitor, timer and owned-pid
  hygiene a teardown needs.

  It takes the table as a plain map and never names `Extension.Table`, so the
  dependency runs one way. It decides no restart: a crashed subscriber is the
  convergence loop's to restart, on the loop's own table.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.ProviderBehaviour

  @doc """
  Health verdict for every plugin on the table. Only `:running` plugins are
  evaluated — a `:prepared` plugin has not acquired upstream resources yet, so
  it is reported `:healthy`, and that verdict is NOT evidence the plugin is
  running (read the phase for that). Providers that export `health/1` are
  queried with `:owned_pid` injected; providers without it default to
  "owned pid alive (or absent) ⇒ healthy".

  A probe that could not be completed — the provider reported
  `{:fail_open, reason}`, raised, or answered with nonsense — is returned as
  `{:fail_open, reason}` and emitted as telemetry naming this plugin. The
  answer travels UNJUDGED: folding uncertainty to healthy is the convergence
  loop's judge, which is the only place a fact becomes a state.
  """
  @spec probe(map()) :: %{binary() => ProviderBehaviour.probe_result()}
  def probe(table) do
    Map.new(table.plugins, fn {plugin_id, plugin} ->
      {plugin_id, plugin_health(table, plugin_id, plugin)}
    end)
  end

  defp plugin_health(table, plugin_id, plugin) do
    case Map.get(table.phases, plugin_id, :prepared) do
      :running -> running_plugin_health(table, plugin_id, plugin)
      _ -> :healthy
    end
  end

  defp running_plugin_health(table, plugin_id, %Plugin{provider: provider, opts: opts}) do
    owned_pid =
      case Map.get(table.owned_pids, plugin_id) do
        {pid, _ref} -> pid
        _ -> nil
      end

    cond do
      not Code.ensure_loaded?(provider) ->
        {:unhealthy, :provider_not_loaded}

      function_exported?(provider, :health, 1) ->
        probe_provider(plugin_id, provider, opts, owned_pid)

      is_nil(owned_pid) ->
        :healthy

      Process.alive?(owned_pid) ->
        :healthy

      true ->
        {:unhealthy, :owned_pid_dead}
    end
  end

  # An uncertain probe is emitted here and nowhere else: a wedged provider is
  # otherwise indistinguishable from a working one at every layer above this
  # call, and this is the only layer that knows which plugin to name. The
  # SHAPE of the answer is normalised by the fact seam, so this function holds
  # no second vocabulary of its own.
  defp probe_provider(plugin_id, provider, opts, owned_pid) do
    fun = fn -> provider.health(Map.put(opts || %{}, :owned_pid, owned_pid)) end

    case ProviderBehaviour.normalise(fun) do
      {:fail_open, reason} -> ProviderBehaviour.fail_open(plugin_id, provider, reason)
      verdict -> verdict
    end
  end

  @doc "The phase a restarted plugin re-enters: `:prepared` when it can activate."
  @spec restarted_phase(Plugin.t()) :: :prepared | :running
  def restarted_phase(%Plugin{provider: provider}) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :activate, 1),
      do: :prepared,
      else: :running
  end

  @doc "Demonitor `ref` if there is one."
  @spec safe_demonitor(reference() | nil) :: :ok
  def safe_demonitor(nil), do: :ok
  def safe_demonitor(ref), do: Process.demonitor(ref, [:flush])

  @doc "Cancel `ref` if there is one."
  @spec safe_cancel_timer(reference() | nil) :: :ok
  def safe_cancel_timer(nil), do: :ok

  def safe_cancel_timer(ref) do
    _ = Process.cancel_timer(ref)
    :ok
  end

  @doc "Stop a tracked owned pid without taking the host down with it."
  @spec safe_kill(pid() | nil) :: :ok
  def safe_kill(nil), do: :ok

  def safe_kill(pid) when is_pid(pid) do
    if Process.alive?(pid) do
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
    end

    :ok
  end
end
