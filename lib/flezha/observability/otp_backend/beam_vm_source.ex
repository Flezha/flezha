defmodule Flezha.Observability.OtpBackend.BeamVmSource do
  @moduledoc """
  Polled source emitting BEAM VM metrics: per-bucket memory, the binary
  allocator's carriers, scheduler utilisation, run-queue lengths, and
  cumulative GC counts. `reading/0` is the one memory reading, which each
  poll emits.

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
    title: "Controller memory and load",
    description:
      "How much memory the controller's Erlang VM uses and how busy it is: " <>
        "schedulers, processes and garbage collection.",
    signals: [
      memory_total:
        {:metric,
         label: "Memory in use",
         meaning: "All the memory the VM holds, in bytes.",
         sentence: "The VM holds {value:byte} in all"},
      memory_processes:
        {:metric,
         label: "Process memory",
         meaning: "Memory held by the VM's processes, in bytes.",
         sentence: "Processes hold {value:byte}"},
      memory_atom: {:metric, label: "Atom memory", meaning: "Memory held by atoms, in bytes."},
      memory_binary:
        {:metric,
         label: "Binary memory",
         meaning: "Memory held by binaries such as message payloads, in bytes."},
      memory_ets:
        {:metric, label: "Table memory", meaning: "Memory held by in-memory tables, in bytes."},
      memory_binary_alloc_carriers:
        {:metric,
         label: "Binary allocator",
         meaning:
           "Memory the binary allocator holds from the OS, in bytes; far above binary " <>
             "memory, it is binaries pinned in carriers rather than in use."},
      run_queue_total:
        {:metric,
         label: "Waiting to run",
         meaning:
           "Processes waiting for a scheduler; a growing number means the VM is overloaded.",
         sentence: "{value:process} waiting to run"},
      gc_count:
        {:metric,
         label: "Garbage collections", meaning: "Garbage collections since the VM started."},
      gc_words_reclaimed:
        {:metric,
         label: "Memory reclaimed",
         meaning: "Words of memory garbage collection has reclaimed since the VM started."},
      process_count:
        {:metric,
         label: "Processes",
         meaning: "Processes alive in the VM.",
         sentence: "{value:process} alive"},
      port_count:
        {:metric,
         label: "Ports",
         meaning: "Ports open: files, sockets and other connections to the outside."},
      scheduler_utilisation:
        {:metric,
         label: "Scheduler load",
         meaning: "How busy the schedulers were since the last reading, from 0 to 1."}
    ]

  alias Flezha.Observability.{Bus, Signal}

  @memory_names for bucket <- [:total, :processes, :atom, :binary, :ets],
                    do: {bucket, :"memory_#{bucket}"}

  @doc """
  The VM's memory and size now, by the name each value is emitted under: the
  memory buckets, the binary allocator's carriers, processes and ports.
  """
  @spec reading() :: %{atom() => non_neg_integer()}
  def reading() do
    mem = :erlang.memory()

    @memory_names
    |> Map.new(fn {bucket, name} -> {name, Keyword.get(mem, bucket, 0)} end)
    |> Map.merge(%{
      memory_binary_alloc_carriers:
        Keyword.get(:recon_alloc.memory(:allocated_types), :binary_alloc, 0),
      process_count: :erlang.system_info(:process_count),
      port_count: :erlang.system_info(:port_count)
    })
  end

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
    emit_reading()
    emit_run_queues()
    emit_gc_counts()
    emit_scheduler_utilisation(state)
  end

  defp emit_reading() do
    for {name, value} <- reading() do
      Bus.emit(
        Signal.new(%{
          kind: :metric,
          source: @source,
          name: name,
          measurements: %{value: value}
        })
      )
    end
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
