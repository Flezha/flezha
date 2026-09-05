defmodule Flezha.Observability.TelemetryBridge do
  @moduledoc """
  Single dispatching `:telemetry` handler that lifts existing
  `[:flezha, ...]` telemetry events onto the observability bus.

  No producer call-site is rewritten. The mapping is two halves: the hand-kept
  lifecycle half in `@mapping`, and a half DERIVED from the convergence loop's
  detector table — every event a detector entry declares is bridged by that
  declaration, so a new detector needs no edit here. Where both halves name one
  event the hand-kept line wins, because its source, name and kind are a
  deliberate choice a derivation cannot know.

  Every `[:flezha, …]` event executed in the controller must be a key of the
  merged mapping or a member of `exempt/0`, which names the change that owes it
  a sink. The suite fails on any other, naming the file, the line and the
  event, and pins `exempt/0`'s length so the list can only shrink.

  A broken bridge must never crash a producer: handler bodies are
  wrapped in `try`. On error the `:telemetry_bridge_failures` counter
  increments and the error is logged once per unique error class.
  """

  use GenServer, restart: :transient

  require Logger

  alias Flezha.Convergence.Probe.Table
  alias Flezha.Observability.{Bus, Counters, Registry, Signal}

  @handler_id :flezha_observability_telemetry_bridge

  # event => {source_atom, name_atom, kind}
  @mapping %{
    [:flezha, :state, :wal, :backpressure] => {:flezha_state, :wal_backpressure, :metric},
    [:flezha, :state, :fsync, :tick] => {:flezha_state, :fsync_tick, :event},
    [:flezha, :boot, :recovery_complete] => {:flezha_boot, :recovery_complete, :event},
    [:flezha, :boot, :reconcile_duplicate] => {:flezha_boot, :reconcile_duplicate, :event},
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
    [:flezha, :state, :credit_anomaly] => {:flezha_state, :credit_anomaly, :event},
    [:flezha, :state, :output_stall] => {:flezha_state, :output_stall, :event},
    [:flezha, :state, :compaction_timeout] => {:flezha_state, :compaction_timeout, :event},
    [:flezha, :state, :compaction_trigger] => {:flezha_state, :compaction_trigger, :event},
    [:flezha, :state, :baseline_seed] => {:flezha_state, :baseline_seed, :event},
    [:flezha, :state, :wal_realign] => {:flezha_state, :wal_realign, :event},
    [:flezha, :state, :stale_writer_refused] => {:flezha_state, :stale_writer_refused, :event},
    # Remote durability tier. Every alarm it documents was a Logger line until
    # these entries existed — including `rpo_exceeded`, which the operator guide
    # calls alarm-grade and reports through `/healthz`.
    [:flezha, :state, :rpo_exceeded] => {:flezha_durability, :rpo_exceeded, :event},
    [:flezha, :state, :ship_fenced] => {:flezha_durability, :ship_fenced, :event},
    [:flezha, :state, :ship_failed] => {:flezha_durability, :ship_failed, :event},
    [:flezha, :state, :artifact_shipped] => {:flezha_durability, :artifact_shipped, :metric},
    [:flezha, :state, :hydrated] => {:flezha_durability, :hydrated, :event},
    [:flezha, :state, :hydration_failed] => {:flezha_durability, :hydration_failed, :event},
    [:flezha, :state, :artifact_missing] => {:flezha_durability, :artifact_missing, :event},
    [:flezha, :state, :durability_gc] => {:flezha_durability, :durability_gc, :metric},
    [:flezha, :leadership, :transition] => {:flezha_leadership, :transition, :event},
    [:flezha, :reconcile, :action] => {:flezha_reconcile, :action, :event},
    [:flezha, :reconcile, :health] => {:flezha_reconcile, :health, :event},
    [:flezha, :stream, :drop] => {:flezha_stream, :drop, :metric},
    [:flezha, :convergence, :intent_mismatch] => {:flezha_convergence, :intent_mismatch, :event},
    [:flezha, :dlq, :parked] => {:flezha_dlq, :dlq_parked_total, :metric},
    [:flezha, :dlq, :retention_dropped] =>
      {:flezha_dlq, :dlq_dropped_by_retention_total, :metric},
    [:flezha, :dlq, :usage] => {:flezha_dlq, :dlq_usage, :metric},
    [:flezha, :plugin, :operative_forward, :delivered] =>
      {:flezha_operative_forward, :delivered, :metric},
    [:flezha, :plugin, :operative_forward, :retry] =>
      {:flezha_operative_forward, :retry, :metric},
    [:flezha, :plugin, :operative_forward, :queue_depth] =>
      {:flezha_operative_forward, :queue_depth, :metric},
    [:flezha, :plugin, :operative_forward, :saturated] =>
      {:flezha_operative_forward, :saturated, :event},
    [:flezha, :plugin, :http_poll, :injected] => {:flezha_http_poll, :injected, :metric},
    [:flezha, :plugin, :http_poll, :unchanged] => {:flezha_http_poll, :unchanged, :metric},
    [:flezha, :plugin, :http_poll, :failed] => {:flezha_http_poll, :failed, :event},
    [:flezha, :operative, :parked] => {:flezha_operative, :operative_parked_total, :metric}
  }

  # Executed `[:flezha, …]` events with no bridge line yet. Every one is
  # lifecycle telemetry whose sink is `count-once-export-by-declaration`'s to
  # declare: that change owns the counters and the export table these feed. The
  # list is a RATCHET — the suite pins its length, so an event may leave it and
  # none may join.
  @exempt_owner :count_once_export_by_declaration

  @exempt [
    [:flezha, :boot, :reconcile_round],
    [:flezha, :cluster, :ready],
    [:flezha, :convergence, :tick],
    [:flezha, :correction, :completed],
    [:flezha, :correction, :refused],
    [:flezha, :correction, :retention_derived],
    [:flezha, :hub, :pid_removed],
    [:flezha, :operative, :requested_spawn],
    [:flezha, :plugin, :kafka, :metadata, :refresh],
    [:flezha, :plugin, :kafka, :produce, :dropped],
    [:flezha, :plugin, :kafka, :produce, :failed],
    [:flezha, :plugin, :kafka, :produce, :ok],
    [:flezha, :plugin, :kafka, :produce, :retry],
    [:flezha, :plugin, :kafka, :topic, :created],
    [:flezha, :plugin, :kafka, :worker, :swapped],
    [:flezha, :runner, :factory_language_conflict],
    [:flezha, :runner, :quota_refused],
    [:flezha, :schedule, :dormant],
    [:flezha, :schedule, :fired],
    [:flezha, :schedule, :hub_start_failed],
    [:flezha, :schedule, :spawn_failed],
    [:flezha, :sdk, :dynamic],
    [:flezha, :sdk, :mailbox],
    [:flezha, :sdk, :retry],
    [:flezha, :sdk, :timers],
    [:flezha, :state, :claim_superseded],
    [:flezha, :state, :compaction_completed],
    [:flezha, :state, :compaction_failed],
    [:flezha, :state, :compaction_gave_up],
    [:flezha, :state, :disk_quota_breach],
    [:flezha, :state, :inbox_duplicate],
    [:flezha, :state, :input_log_frozen_watermark],
    [:flezha, :state, :input_log_syncer_died],
    [:flezha, :state, :record_frame, :refused],
    [:flezha, :state, :record_frame, :repaired],
    [:flezha, :state, :restore_refused],
    [:flezha, :state, :schema_migration],
    [:flezha, :state, :wal_moved],
    [:flezha, :state, :wal_pull_failed],
    [:flezha, :state, :wal_pulled],
    [:flezha, :state, :wal_rewind],
    [:flezha, :stream, :fanout_warning],
    [:flezha, :workflow, :completed],
    [:flezha, :workflow, :rehydrating],
    [:flezha, :workflow, :rewind],
    [:flezha, :ws, :reconcile]
  ]

  # Derived at COMPILE time from the detector table, so adding a detector
  # recompiles this module and its event is bridged with no edit here.
  @derived Map.new(Table.events(), fn event ->
             {event, {:flezha_health, List.last(event), :event}}
           end)

  @all_mapping Map.merge(@derived, @mapping)

  # ---------- Public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  The bridged source atoms. Pre-registered with the Registry on
  bridge start so `Bus` can enforce cardinality budgets against them.
  """
  @spec bridged_sources() :: [{atom(), [Signal.kind()]}]
  def bridged_sources() do
    mapping()
    |> Map.values()
    |> Enum.group_by(fn {source, _name, _kind} -> source end, fn {_s, _n, k} -> k end)
    |> Enum.map(fn {source, kinds} -> {source, Enum.uniq(kinds)} end)
  end

  @doc false
  @spec handler_id() :: atom()
  def handler_id(), do: @handler_id

  @doc """
  The bridged events: the derived half under the hand-kept one. A detector's
  declared event maps to `{:flezha_health, <last segment>, :event}` unless the
  hand-kept half already names it.
  """
  @spec mapping() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def mapping(), do: @all_mapping

  @doc "The half derived from the convergence loop's detector declarations."
  @spec derived() :: %{[atom()] => {atom(), atom(), Signal.kind()}}
  def derived(), do: @derived

  @doc """
  Executed `[:flezha, …]` events that reach no sink yet, each naming the change
  that owes it one. A ratchet: the suite pins this list's length, so an event
  can leave it and none may join.
  """
  @spec exempt() :: [[atom()]]
  def exempt(), do: @exempt

  @doc "The change that owes every exempt event a sink."
  @spec exempt_owner() :: atom()
  def exempt_owner(), do: @exempt_owner

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
    case Map.fetch(@all_mapping, event) do
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
    class = Counters.error_class(error)

    if Counters.first_seen?({:telemetry_bridge, event, class}) do
      Logger.error(
        "[observability.telemetry_bridge] error event=#{inspect(event)} " <>
          "class=#{inspect(class)} error=#{inspect(error)} " <>
          "stack=#{Exception.format_stacktrace(stacktrace)}"
      )
    end
  end

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

    events = Map.keys(@all_mapping)

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
