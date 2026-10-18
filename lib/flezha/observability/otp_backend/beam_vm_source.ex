defmodule Flezha.Observability.OtpBackend.BeamVmSource do
  @moduledoc """
  Polled source emitting BEAM VM metrics: per-bucket memory, scheduler
  utilisation, run-queue lengths, and cumulative GC counts.

  Registers as `:beam_vm`, kind `[:metric]`. Cardinality budget is a
  small constant: every emit carries either no metadata or a single
  enum-shaped key, so the label-shape footprint per metric name is
  ≤ 2 distinct shapes regardless of cluster size.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :beam_vm`.
  """

  use Flezha.Observability.OtpBackend.Poller,
    source: :beam_vm,
    kinds: [:metric],
    cardinality_budget: 50,
    default_poll_ms: 1_000,
    area: :machine,
    description:
      "Controller memory and load: how much memory the controller's Erlang VM uses and how busy it is — schedulers, processes and garbage collection.",
    signals: [
      memory_total: {:metric, "Bytes of memory the VM holds in all."},
      memory_processes: {:metric, "Bytes held by Erlang processes."},
      memory_atom: {:metric, "Bytes held by atoms."},
      memory_binary: {:metric, "Bytes held by binaries."},
      memory_ets: {:metric, "Bytes held by ETS tables."},
      run_queue_total: {:metric, "Processes waiting for a scheduler."},
      gc_count: {:metric, "Garbage collections since the VM started."},
      gc_words_reclaimed:
        {:metric, "Words reclaimed by garbage collection since the VM started."},
      process_count: {:metric, "Erlang processes alive."},
      port_count: {:metric, "Ports open."},
      scheduler_utilisation: {:metric, "Share of time the schedulers were busy, 0 to 1."}
    ]

  alias Flezha.Observability.{Bus, Signal}

  @memory_buckets [:total, :processes, :atom, :binary, :ets]

  @impl Flezha.Observability.OtpBackend.Poller
  def init_state(_opts) do
    # Scheduler-wall-time sampling must be on before the first delta.
    _ = :erlang.system_flag(:scheduler_wall_time, true)
    %{last_scheduler_sample: nil}
  end

  @impl GenServer
  def terminate(_reason, _state) do
    _ = :erlang.system_flag(:scheduler_wall_time, false)
    :ok
  end

  @impl Flezha.Observability.OtpBackend.Poller
  def poll(state) do
    emit_memory()
    emit_run_queues()
    emit_gc_counts()
    emit_process_counts()
    emit_scheduler_utilisation(state)
  end

  defp emit_memory() do
    mem = :erlang.memory()

    Enum.each(@memory_buckets, fn bucket ->
      value = Keyword.get(mem, bucket, 0)
      name = String.to_atom("memory_" <> Atom.to_string(bucket))

      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: @source,
          name: name,
          measurements: %{value: value}
        })
      )
    end)
  end

  defp emit_run_queues() do
    total = :erlang.statistics(:total_run_queue_lengths_all)

    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: :run_queue_total,
        measurements: %{value: total}
      })
    )
  end

  defp emit_gc_counts() do
    {gc_count, words_reclaimed, _} = :erlang.statistics(:garbage_collection)

    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: :gc_count,
        measurements: %{count: gc_count}
      })
    )

    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: :gc_words_reclaimed,
        measurements: %{count: words_reclaimed}
      })
    )
  end

  defp emit_process_counts() do
    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: :process_count,
        measurements: %{value: :erlang.system_info(:process_count)}
      })
    )

    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: :port_count,
        measurements: %{value: :erlang.system_info(:port_count)}
      })
    )
  end

  defp emit_scheduler_utilisation(%{last_scheduler_sample: nil} = state) do
    sample = :erlang.statistics(:scheduler_wall_time)
    %{state | last_scheduler_sample: sample}
  end

  defp emit_scheduler_utilisation(%{last_scheduler_sample: prev} = state) do
    current = :erlang.statistics(:scheduler_wall_time)

    if is_list(current) and is_list(prev) do
      utilisation = scheduler_utilisation(prev, current)

      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: @source,
          name: :scheduler_utilisation,
          measurements: %{value: utilisation}
        })
      )
    end

    %{state | last_scheduler_sample: current}
  end

  defp scheduler_utilisation(prev, current) do
    prev_map =
      Enum.into(prev, %{}, fn {sched_id, active, total} -> {sched_id, {active, total}} end)

    {active_delta, total_delta} =
      Enum.reduce(current, {0, 0}, fn {sched_id, active, total}, {a, t} ->
        case Map.get(prev_map, sched_id) do
          {prev_active, prev_total} ->
            {a + (active - prev_active), t + (total - prev_total)}

          _ ->
            {a, t}
        end
      end)

    if total_delta > 0, do: active_delta / total_delta, else: 0.0
  end
end
