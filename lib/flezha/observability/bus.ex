defmodule Flezha.Observability.Bus do
  @moduledoc """
  Producer-side dispatch.

  `emit/1` runs in the calling process. It enforces the per-source
  cardinality budget on `:metric` signals, looks up the set of sinks
  whose `filter/0` accepts the signal via a direct ETS read, and
  fans out either via `Process.send/3` (for `:drop` overflow policy)
  or `GenServer.call/3` (for `:block` overflow policy).

  Hot-path safety: an emit with no interested sinks is a no-op (no
  ETS writes, no process messages). Any exception inside dispatch is
  caught — `:dispatcher_failure_count` is incremented and the error
  is logged at most once per unique error class. A broken
  observability subsystem must never crash a producer.
  """

  require Logger

  alias Flezha.Observability.{Counters, Registry, Signal}

  @sinks :flezha_observability_sinks
  @system_source :observability_registry

  @doc """
  Emit a signal. Always returns `:ok`. Never raises.
  """
  @spec emit(Signal.t()) :: :ok
  def emit(%Signal{} = signal) do
    try do
      do_emit(signal)
    rescue
      e ->
        handle_dispatch_error(e, __STACKTRACE__)
        :ok
    catch
      kind, reason ->
        handle_dispatch_error({kind, reason}, __STACKTRACE__)
        :ok
    end
  end

  defp do_emit(%Signal{kind: kind, source: source} = signal) do
    case Registry.lookup_source(source) do
      {:ok, %{status: :disabled}} ->
        :ok

      {:ok, %{kinds: kinds} = _entry} ->
        cond do
          kind not in kinds ->
            emit_system_event(:source_kind_violation, %{
              source: source,
              attempted_kind: kind
            })

          kind == :metric ->
            case Registry.check_cardinality(source, signal.metadata) do
              :ok ->
                fan_out(signal)

              {:dropped, :cardinality_budget_exceeded, :first} ->
                emit_system_event(:cardinality_budget_exceeded, %{source: source})

              {:dropped, :cardinality_budget_exceeded, :repeat} ->
                :ok
            end

          true ->
            fan_out(signal)
        end

      :error ->
        # Unregistered source — deliver to interested sinks without
        # cardinality enforcement so bootstrap-time emits (e.g.
        # before bridges register their sources) still flow.
        fan_out(signal)
    end
  end

  defp fan_out(%Signal{} = signal) do
    case interested_sinks(signal) do
      [] ->
        :ok

      sinks ->
        Enum.each(sinks, fn entry -> dispatch_to_sink(entry, signal) end)
        :ok
    end
  end

  defp interested_sinks(signal) do
    case :ets.whereis(@sinks) do
      :undefined ->
        []

      _ ->
        :ets.tab2list(@sinks)
        |> Enum.flat_map(fn {_name, entry} ->
          if accepts?(entry.filter, signal), do: [entry], else: []
        end)
    end
  end

  defp accepts?(%{kinds: kinds_f, sources: sources_f}, %Signal{kind: k, source: s}) do
    kind_match?(kinds_f, k) and source_match?(sources_f, s)
  end

  defp accepts?(_filter, _signal), do: false

  defp kind_match?(:any, _), do: true
  defp kind_match?(list, k) when is_list(list), do: k in list
  defp kind_match?(_, _), do: false

  defp source_match?(:any, _), do: true
  defp source_match?(list, s) when is_list(list), do: s in list
  defp source_match?(_, _), do: false

  defp dispatch_to_sink(%{pid: nil}, _signal), do: :ok

  defp dispatch_to_sink(
         %{
           name: name,
           pid: pid,
           overflow: :drop,
           mailbox_high_watermark: watermark
         },
         signal
       ) do
    if mailbox_full?(pid, watermark) do
      Registry.bump_sink_counter(name, :dropped)
      :ok
    else
      _ = Process.send(pid, {:signal, signal}, [:noconnect])
      :ok
    end
  end

  defp dispatch_to_sink(
         %{pid: pid, overflow: :block, block_timeout_ms: timeout},
         signal
       ) do
    # A sink that emits a signal of its own (e.g. OTLP exporter
    # publishing its `:otlp_batch_dropped` event) must not block on
    # itself — that would deadlock. Fall back to an async cast.
    if pid == self() do
      send(pid, {:signal, signal})
      :ok
    else
      _ = GenServer.call(pid, {:signal, signal}, timeout)
      :ok
    end
  end

  defp mailbox_full?(pid, watermark) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} -> len >= watermark
      nil -> true
    end
  end

  # The foundation's own diagnostics, both dispatched at one depth: suppressed
  # when an operator disables the source they carry, and never subject to its
  # own kind check, which no `:event` on `@system_source` can fail.
  defp emit_system_event(name, metadata) do
    signal =
      Signal.new(%{
        kind: :event,
        source: @system_source,
        name: name,
        measurements: %{count: 1},
        metadata: metadata
      })

    case Registry.lookup_source(@system_source) do
      {:ok, %{status: :disabled}} -> :ok
      _ -> fan_out(signal)
    end
  end

  defp handle_dispatch_error(error, stacktrace) do
    Counters.bump(:dispatcher_failure_count)

    class = Counters.error_class(error)

    # A sink that is shutting down (subsystem teardown, reconfigure) surfaces as
    # an exit with :noproc/:shutdown/:normal — expected, not a producer fault.
    # The counter still records it; skip the error log so it stays quiet.
    if not benign_sink_exit?(error) and Counters.first_seen?({:bus_dispatch, class}) do
      Logger.error(
        "[observability.bus] dispatch error class=#{inspect(class)} " <>
          "error=#{inspect(error)} stack=#{Exception.format_stacktrace(stacktrace)}"
      )
    end
  end

  defp benign_sink_exit?({:exit, reason}), do: benign_exit_reason?(reason)
  defp benign_sink_exit?(_), do: false

  defp benign_exit_reason?(reason) when reason in [:noproc, :normal, :shutdown], do: true
  defp benign_exit_reason?({reason, _}) when reason in [:noproc, :normal, :shutdown], do: true
  defp benign_exit_reason?(_), do: false
end
