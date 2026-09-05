defmodule Flezha.Plugin.Extension.Table.Health do
  @moduledoc """
  The extension table's health verdicts, and the monitor, timer and owned-pid
  hygiene a teardown needs.

  It takes the table as a plain map and never names `Extension.Table`, so the
  dependency runs one way. It decides no restart: a crashed subscriber is the
  convergence loop's to restart, on the loop's own table.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.ProviderBehaviour

  @doc """
  Health verdict for every plugin on the table. Only `:running` plugins are
  evaluated: a `:prepared` plugin has not acquired upstream resources yet, so it
  is reported `:healthy`, and that verdict is NOT evidence the plugin is running
  (read the phase for that). Every provider is queried through its declared
  `health/1`, with the processes the table started injected; a provider that
  overrides nothing takes the default, "the worker alive, or absent, is
  healthy".

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

  # The lifecycle record is read as a plain map, never through its owner: the
  # dependency between this module and the table runs one way, and naming the
  # table here would close the loop.
  defp plugin_health(table, plugin_id, plugin) do
    case Map.get(table.entries, plugin_id) do
      %{phase: :running} = entry -> running_plugin_health(entry, plugin_id, plugin)
      _ -> :healthy
    end
  end

  # An uncertain probe is emitted here and nowhere else: a wedged provider is
  # otherwise indistinguishable from a working one at every layer above this
  # call, and this is the only layer that knows which plugin to name. The SHAPE
  # of the answer is normalised by the fact seam, so this function holds no
  # second vocabulary of its own.
  defp running_plugin_health(entry, plugin_id, %Plugin{provider: provider, opts: opts}) do
    if Code.ensure_loaded?(provider) do
      opts = with_worker_pids(entry, opts)

      case ProviderBehaviour.normalise(fn -> provider.health(opts) end) do
        {:fail_open, reason} -> ProviderBehaviour.fail_open(plugin_id, provider, reason)
        verdict -> verdict
      end
    else
      {:unhealthy, :provider_not_loaded}
    end
  end

  @doc """
  A provider's stored opts with the processes the table started injected: the
  shape `health/1`, `inspect_state/1` and `terminate_provider/1` all read. The
  ONE injection site, because the table started the worker and so knows its pid.
  """
  @spec with_worker_pids(Entry.t() | map(), map() | nil) :: map()
  def with_worker_pids(entry, opts) do
    entry = if is_struct(entry, Entry), do: entry, else: struct(Entry, entry)

    Map.merge(opts || %{}, %{owned_pid: Entry.owned_pid(entry), workers: Entry.pids(entry)})
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
end
