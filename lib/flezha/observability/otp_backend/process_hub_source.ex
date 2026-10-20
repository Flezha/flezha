defmodule Flezha.Observability.OtpBackend.ProcessHubSource do
  @moduledoc """
  Polled source emitting per-hub ProcessHub metrics.

  Enumerates active hubs via the `Flezha.Service.HubManager` /
  `Flezha.Identificator` surface and queries ProcessHub for
  registered-process counts and recovery state.

  Cardinality math: the two hub-wide metrics carry one shape per hub, so a
  cluster with N namespaces emits N of them; `runner_bound_relays` carries
  one shape per runner per hub, so it emits N x runners. The declared budget is
  sized for a fleet of a few hundred runner-namespace pairs, which is what the
  default per-source budget allows too.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :processhub`.
  """

  use Flezha.Observability.OtpBackend.Poller,
    source: :processhub,
    kinds: [:metric],
    cardinality_budget: 1_000,
    default_poll_ms: 1_000,
    area: :machine,
    title: "Namespace hubs",
    description:
      "What each namespace's hub holds, whether it is recovering, and how relays spread over runners.",
    signals: [
      registered_processes:
        {:metric,
         label: "Processes held",
         meaning: "Processes, relays among them, that a namespace's hub keeps track of.",
         sentence: "{hub} holds {value:process}"},
      pending_recoveries:
        {:metric,
         label: "Recovery pending",
         meaning: "1 while a namespace's hub is bringing its relays back, otherwise 0.",
         sentence: "{hub} has {value:recovery} pending"},
      runner_bound_relays:
        {:metric,
         label: "Relays per runner",
         meaning: "How many of a namespace's relays one runner serves.",
         sentence: "{runner} serves {value:relay} in {hub}"}
    ]

  alias Flezha.Identificator
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Load
  alias Flezha.Service.HubManager
  alias ProcessHub.Service.Recovery, as: HubRecovery

  @impl Flezha.Observability.OtpBackend.Poller
  def poll(state) do
    namespaces =
      try do
        HubManager.get_running_namespaces()
      catch
        :exit, _ -> []
      end

    Enum.each(namespaces, &emit_for_namespace/1)
    state
  end

  defp emit_for_namespace(namespace) do
    hub = Identificator.hub(namespace)

    registered = length(HubManager.local_children(namespace, :all))
    emit_metric(:registered_processes, %{value: registered}, %{hub: hub})

    recovery_state = safe_recovery_state(hub)

    emit_metric(:pending_recoveries, %{value: pending_recoveries_value(recovery_state)}, %{
      hub: hub
    })

    emit_bound_relays(namespace, hub)
  end

  # The spread the placement cap governs, read from the registry's runner tags
  # rather than from the cap's own cached count — that count carries handouts
  # that may never have started, and a user is owed the binding rather than
  # the intention. `Load.counts/2` seeds every active runner, so a runner
  # holding nothing reports `0` instead of vanishing from the exposition.
  defp emit_bound_relays(namespace, hub) do
    namespace
    |> HubManager.registry_dump()
    |> Load.counts(safe_active_runners(namespace))
    |> Enum.each(fn {runner_id, bound} ->
      emit_metric(:runner_bound_relays, %{value: bound}, %{hub: hub, runner: runner_id})
    end)
  end

  # The fleet is one serialized process that may be inside a runner connect, so
  # a poll waits the same short budget every other poller does rather than the
  # manager's own.
  defp safe_active_runners(namespace) do
    try do
      FleetManager.active_runners(namespace, @default_poll_ms)
    catch
      _, _ -> []
    end
  end

  defp safe_recovery_state(hub) do
    try do
      HubRecovery.recovery_state(hub)
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
