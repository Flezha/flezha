defmodule Flezha.Plugin.Extension.Table.Health do
  @moduledoc """
  The extension table's health verdicts and its restart-backoff arithmetic:
  `monitors`, `retries` and `started_at` are the only fields it touches.

  It takes the table as a plain map and never names `Extension.Table`, so the
  dependency runs one way — deciding to restart is here, performing one is the
  table's.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.ProviderBehaviour

  require Logger

  # Restart backoff: only reset the retry counter once an owned pid has stayed
  # up at least this long, so a provider that fails fast (e.g. a WS whose
  # upstream keeps 429-ing) accumulates backoff instead of hammering.
  @stable_uptime_ms 60_000
  @max_retry_delay_ms 60_000
  @retry_base_delay_ms 4_000

  @doc """
  Health verdict for every plugin on the table. Only `:running` plugins are
  evaluated — a `:prepared` plugin has not acquired upstream resources yet, so
  it is reported `:healthy`, and that verdict is NOT evidence the plugin is
  running (read the phase for that). Providers that export `health/1` are
  queried with `:owned_pid` injected; providers without it default to
  "owned pid alive (or absent) ⇒ healthy".

  A probe that could not be completed — the provider reported
  `{:fail_open, reason}`, raised, or answered with nonsense — is reported
  `:healthy` so the reconciler never escalates on uncertainty, AND emitted as
  telemetry naming this plugin.
  """
  @spec probe(map()) :: %{binary() => ProviderBehaviour.health_verdict()}
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
        safe_health(plugin_id, provider, opts, owned_pid)

      is_nil(owned_pid) ->
        :healthy

      Process.alive?(owned_pid) ->
        :healthy

      true ->
        {:unhealthy, :owned_pid_dead}
    end
  end

  # An uncertain probe reports healthy so the reconciler never escalates on one
  # — but it is emitted, because a wedged provider is otherwise
  # indistinguishable from a working one at every layer above this call. This is
  # the single place that happens: a provider reports its own uncertainty as
  # `{:fail_open, reason}` and this is the only layer that knows which plugin
  # to name.
  defp safe_health(plugin_id, provider, opts, owned_pid) do
    case provider.health(Map.put(opts || %{}, :owned_pid, owned_pid)) do
      :healthy -> :healthy
      {:unhealthy, _reason} = unhealthy -> unhealthy
      {:fail_open, reason} -> ProviderBehaviour.fail_open(plugin_id, provider, reason)
      other -> ProviderBehaviour.fail_open(plugin_id, provider, {:invalid_return, other})
    end
  rescue
    error -> ProviderBehaviour.fail_open(plugin_id, provider, {:raised, error})
  catch
    kind, reason -> ProviderBehaviour.fail_open(plugin_id, provider, {kind, reason})
  end

  @doc "Drop the retry counter once the owned pid has stayed up long enough."
  @spec reset_retries_if_stable(map(), binary()) :: map()
  def reset_retries_if_stable(table, plugin_id) do
    case Map.get(table.started_at, plugin_id) do
      nil ->
        table

      started_at ->
        uptime = :erlang.monotonic_time(:millisecond) - started_at

        if uptime >= @stable_uptime_ms do
          %{table | retries: Map.delete(table.retries, plugin_id)}
        else
          table
        end
    end
  end

  @doc "Arm the next restart attempt for `plugin_id` under the backoff ladder."
  @spec schedule_retry(map(), binary()) :: map()
  def schedule_retry(table, plugin_id) do
    count = Map.get(table.retries, plugin_id, 0)
    delay = retry_delay(count)

    Logger.info(
      "[Extension.Table] scheduling retry plugin_id=#{plugin_id} delay_ms=#{delay} attempt=#{count + 1}"
    )

    Process.send_after(self(), {:retry_plugin_restart, plugin_id}, delay)
    %{table | retries: Map.put(table.retries, plugin_id, count + 1)}
  end

  @doc "The phase a restarted plugin re-enters: `:prepared` when it can activate."
  @spec restarted_phase(Plugin.t()) :: :prepared | :running
  def restarted_phase(%Plugin{provider: provider}) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :activate, 1),
      do: :prepared,
      else: :running
  end

  defp retry_delay(count) do
    base = Application.get_env(:flezha, :plugin_retry_base_delay_ms, @retry_base_delay_ms)

    # Cap the exponent, not just the product: an unbounded count would build a
    # bignum the clamp then throws away.
    ProviderBehaviour.retry_backoff_ms(base, @max_retry_delay_ms, min(count, 20) + 1)
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
