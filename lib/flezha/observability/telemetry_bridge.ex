defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten — the bridge listens for every
  event named in `@mapping` and emits a `Signal` per execution. New
  bridged events are a one-line change to the mapping table.

  A broken bridge must never crash a producer: handler bodies are
  wrapped in `try`. On error the `:telemetry_bridge_failures` counter
  increments and the error is logged once per unique error class.
  """

  use GenServer

  require Logger

  alias Flezha.Observability.{Bus, Counters, Registry, Signal}

  @handler_id :flezha_observability_telemetry_bridge

  # event => {source_atom, name_atom, kind}
  @mapping %{
    [:flezha, :state, :wal, :backpressure] => {:flezha_state, :wal_backpressure, :metric},
    [:flezha, :state, :fsync, :tick] => {:flezha_state, :fsync_tick, :event},
    [:flezha, :boot, :recovery_complete] => {:flezha_boot, :recovery_complete, :event},
    [:flezha, :shutdown, :requested] => {:flezha_shutdown, :requested, :event},
    [:flezha, :shutdown, :phase_started] => {:flezha_shutdown, :phase_started, :event},
    [:flezha, :shutdown, :phase_completed] => {:flezha_shutdown, :phase_completed, :event},
    [:flezha, :shutdown, :drain_progress] => {:flezha_shutdown, :drain_progress, :event},
    [:flezha, :shutdown, :timeout] => {:flezha_shutdown, :timeout, :event},
    [:flezha, :shutdown, :complete] => {:flezha_shutdown, :complete, :event},
    [:flezha, :plugin_installer, :load_failed] =>
      {:flezha_plugin_installer, :load_failed, :event},
    [:flezha, :plugin_installer, :store_opened] =>
      {:flezha_plugin_installer, :store_opened, :event},
    [:flezha, :plugin_installer, :unsigned_install] =>
      {:flezha_plugin_installer, :unsigned_install, :event},
    [:flezha, :ws, :idle_reconnect] => {:flezha_ws, :idle_reconnect, :event},
    [:flezha, :ws, :resubscribe] => {:flezha_ws, :resubscribe, :event},
    [:flezha, :state, :seq_gap] => {:flezha_state, :seq_gap, :event},
    [:flezha, :state, :output_stall] => {:flezha_state, :output_stall, :event},
    [:flezha, :state, :compaction_timeout] => {:flezha_state, :compaction_timeout, :event},
    [:flezha, :state, :wal_realign] => {:flezha_state, :wal_realign, :event},
    [:flezha, :leadership, :transition] => {:flezha_leadership, :transition, :event},
    [:flezha, :reconcile, :action] => {:flezha_reconcile, :action, :event},
    [:flezha, :reconcile, :health] => {:flezha_reconcile, :health, :event},
    [:flezha, :stream, :drop] => {:flezha_stream, :drop, :metric},
    [:flezha, :data_plane, :stall_localized] => {:flezha_data_plane, :stall_localized, :event}
  }

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :worker,
      shutdown: 5_000
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The bridged source atoms. Pre-registered with the Registry on
  bridge start so `Bus` can enforce cardinality budgets against them.
  """
  @spec bridged_sources() :: [{atom(), [Signal.kind()]}]
  def bridged_sources() do
    @mapping
    |> Map.values()
    |> Enum.group_by(fn {source, _name, _kind} -> source end, fn {_s, _n, k} -> k end)
    |> Enum.map(fn {source, kinds} -> {source, Enum.uniq(kinds)} end)
  end

  @doc false
  @spec handler_id() :: atom()
  def handler_id(), do: @handler_id

  @doc false
  @spec mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def mapping(), do: @mapping

  # ---------- :telemetry handler entry point ----------

  @doc false
  def handle_event(event, measurements, metadata, _config) do
    try do
      do_handle_event(event, measurements, metadata)
    rescue
      e -> handle_error(event, e, __STACKTRACE__)
    catch
      kind, reason -> handle_error(event, {kind, reason}, __STACKTRACE__)
    end

    :ok
  end

  defp do_handle_event(event, measurements, metadata) do
    case Map.fetch(@mapping, event) do
      {:ok, {source, name, kind}} ->
        signal =
          Signal.new(%{
            kind: kind,
            source: source,
            name: name,
            measurements: measurements,
            metadata: metadata
          })

        Bus.emit(signal)

      :error ->
        :ok
    end
  end

  defp handle_error(event, error, stacktrace) do
    Counters.bump(:telemetry_bridge_failures)
    class = error_class(error)

    if Counters.first_seen?({:telemetry_bridge, event, class}) do
      Logger.error(
        "[observability.telemetry_bridge] error event=#{inspect(event)} " <>
          "class=#{inspect(class)} error=#{inspect(error)} " <>
          "stack=#{Exception.format_stacktrace(stacktrace)}"
      )
    end
  end

  defp error_class(%{__struct__: mod}), do: mod
  defp error_class({kind, _reason}) when kind in [:exit, :throw], do: kind
  defp error_class(other), do: other

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    # Pre-register the bridged sources so the Bus + Registry track
    # them (cardinality budget, source-status, etc.).
    Enum.each(bridged_sources(), fn {source, kinds} ->
      _ =
        Registry.register_source(%{
          name: source,
          kinds: kinds,
          flavour: :declarative
        })
    end)

    events = Map.keys(@mapping)

    :ok =
      :telemetry.attach_many(@handler_id, events, &__MODULE__.handle_event/4, nil)

    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach(@handler_id)
    :ok
  end
end
