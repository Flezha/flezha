defmodule Flezha.Observability.OtpBackend.ProcessHubSource do
  @moduledoc """
  Polled source emitting per-hub ProcessHub metrics.

  Enumerates active hubs via the `Flezha.Service.HubManager` /
  `Flezha.Service.Identificator` surface and queries ProcessHub for
  registered-process counts and recovery state.

  Cardinality math: one metadata shape per hub. A cluster with N
  namespaces emits N distinct shapes per metric name. Default
  budget `1_000` is sufficient for any practical cluster — the
  source declares an explicit `200` here, sized for the typical
  namespace count, with headroom.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :processhub`.
  """

  use Flezha.Observability.OtpBackend.Source, source: :processhub, default_poll_ms: 1_000

  alias Flezha.Observability.{Bus, Registry, Signal}
  alias Flezha.Service.{HubManager, Identificator}

  @impl GenServer
  def init(_opts) do
    Registry.register_source(%{
      name: @source,
      kinds: [:metric],
      flavour: :process_backed,
      cardinality_budget: 200
    })

    interval = poll_interval_ms()
    Process.send_after(self(), :poll, interval)
    {:ok, %{interval: interval}}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    poll_hubs()
    Process.send_after(self(), :poll, state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Internals ----------

  defp poll_hubs() do
    namespaces =
      try do
        HubManager.get_running_namespaces()
      catch
        :exit, _ -> []
      end

    Enum.each(namespaces, &emit_for_namespace/1)
  end

  defp emit_for_namespace(namespace) do
    hub = Identificator.hub(namespace)

    registered = safe_process_count(hub)
    emit_metric(:registered_processes, %{value: registered}, %{hub: hub})

    recovery_state = safe_recovery_state(hub)

    emit_metric(:pending_recoveries, %{value: pending_recoveries_value(recovery_state)}, %{
      hub: hub
    })
  end

  defp safe_process_count(hub) do
    try do
      hub
      |> ProcessHub.process_list(:local)
      |> length()
    catch
      _, _ -> 0
    end
  end

  defp safe_recovery_state(hub) do
    try do
      ProcessHub.Service.Recovery.recovery_state(hub)
    catch
      _, _ -> :normal
    end
  end

  defp pending_recoveries_value(:recovering), do: 1
  defp pending_recoveries_value(:recovery_pending), do: 1
  defp pending_recoveries_value(_), do: 0

  defp emit_metric(name, measurements, metadata) do
    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: name,
        measurements: measurements,
        metadata: metadata
      })
    )
  end
end
