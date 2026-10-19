defmodule Flezha.Observability.OtpBackend.HostSource do
  @moduledoc """
  Polled source emitting OS-process metrics read from Linux `/proc`.

  On non-Linux platforms the source emits a single `:event`
  (`:host_source_degraded`) on its first poll and transitions to a
  no-op state. RSS and FD count are gauges; CPU time fields are
  cumulative counters; a derived `:cpu_utilisation` gauge is emitted
  from the second poll onward.

  Default poll interval `5_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :host`.
  """

  use Flezha.Observability.OtpBackend.Poller,
    source: :host,
    kinds: [:metric, :event],
    cardinality_budget: 50,
    default_poll_ms: 5_000,
    area: :machine,
    title: "Controller process",
    description:
      "The controller's process as the operating system sees it: CPU, memory, open files and host load.",
    signals: [
      cpu_user_us:
        {:metric,
         label: "CPU in own code",
         meaning:
           "CPU time the controller spent in its own code since it started, in microseconds."},
      cpu_system_us:
        {:metric,
         label: "CPU in the kernel",
         meaning: "CPU time the controller spent in the kernel since it started, in microseconds."},
      cpu_utilisation:
        {:metric,
         label: "CPU use",
         meaning: "How much of one CPU the controller used since the last reading, from 0 to 1."},
      rss_bytes:
        {:metric,
         label: "Memory in use",
         meaning: "Memory the controller's process holds in RAM, in bytes.",
         sentence: "The controller holds {value:byte} in memory"},
      open_fds:
        {:metric,
         label: "Open files",
         meaning: "Files and sockets the controller has open.",
         sentence: "{value:file} open"},
      load_avg_1m:
        {:metric,
         label: "Host load, 1 minute", meaning: "The host's load average over the last minute."},
      load_avg_5m:
        {:metric,
         label: "Host load, 5 minutes",
         meaning: "The host's load average over the last 5 minutes."},
      load_avg_15m:
        {:metric,
         label: "Host load, 15 minutes",
         meaning: "The host's load average over the last 15 minutes."},
      host_source_degraded:
        {:event,
         label: "Cannot read host",
         outcome: :failure,
         meaning:
           "This platform does not expose the host's figures, so none are reported; sent once."}
    ]

  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.OtpBackend.ProcParser

  @stat_path "/proc/self/stat"
  @status_path "/proc/self/status"
  @loadavg_path "/proc/loadavg"
  @fd_dir "/proc/self/fd"

  @impl Flezha.Observability.OtpBackend.Poller
  def init_state(_opts), do: %{status: :probing, prev_cpu_jiffies: nil}

  @impl Flezha.Observability.OtpBackend.Poller
  def poll(%{status: :degraded} = state), do: state

  def poll(state) do
    case File.read(@stat_path) do
      {:ok, stat_bytes} ->
        handle_task_read(state, stat_bytes)

      {:error, _reason} ->
        emit_degraded()
        %{state | status: :degraded}
    end
  end

  defp handle_task_read(state, stat_bytes) do
    {state, current_jiffies} =
      case ProcParser.parse_stat(String.trim(stat_bytes)) do
        {:ok, stat} ->
          emit_cpu_counters(stat)
          {emit_cpu_utilisation(state, stat), stat.utime + stat.stime}

        _ ->
          {state, state.prev_cpu_jiffies}
      end

    case File.read(@status_path) do
      {:ok, status_bytes} ->
        case ProcParser.parse_status(status_bytes) do
          {:ok, %{vm_rss_bytes: rss}} when is_integer(rss) and rss > 0 ->
            emit_metric(:rss_bytes, %{value: rss})

          _ ->
            :ok
        end

      _ ->
        :ok
    end

    case open_fd_count() do
      n when is_integer(n) and n >= 0 -> emit_metric(:open_fds, %{value: n})
      _ -> :ok
    end

    case File.read(@loadavg_path) do
      {:ok, loadavg_bytes} ->
        case ProcParser.parse_loadavg(loadavg_bytes) do
          {:ok, %{load_1m: l1, load_5m: l5, load_15m: l15}} ->
            emit_metric(:load_avg_1m, %{value: l1})
            emit_metric(:load_avg_5m, %{value: l5})
            emit_metric(:load_avg_15m, %{value: l15})

          _ ->
            :ok
        end

      _ ->
        :ok
    end

    %{state | status: :active, prev_cpu_jiffies: current_jiffies}
  end

  defp emit_cpu_counters(stat) do
    # Linux jiffies are 100 Hz by default — surface as microseconds so
    # the unit is consistent with the rest of the foundation's
    # measurement vocabulary.
    emit_metric(:cpu_user_us, %{count: jiffies_to_us(stat.utime)})
    emit_metric(:cpu_system_us, %{count: jiffies_to_us(stat.stime)})
  end

  defp emit_cpu_utilisation(%{prev_cpu_jiffies: nil} = state, _stat), do: state

  defp emit_cpu_utilisation(%{prev_cpu_jiffies: prev, interval: interval_ms} = state, stat) do
    current = stat.utime + stat.stime
    delta_jiffies = current - prev
    # interval is in ms; one jiffy ≈ 10 ms on stock Linux (100Hz).
    elapsed_jiffies = max(interval_ms / 10, 1.0)
    util = max(min(delta_jiffies / elapsed_jiffies, 1.0), 0.0)
    emit_metric(:cpu_utilisation, %{value: util})
    state
  end

  defp open_fd_count() do
    case File.ls(@fd_dir) do
      {:ok, entries} -> length(entries)
      _ -> nil
    end
  end

  defp jiffies_to_us(jiffies), do: jiffies * 10_000

  defp emit_metric(name, measurements) do
    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: name,
        measurements: measurements
      })
    )
  end

  defp emit_degraded() do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :host_source_degraded,
        measurements: %{count: 1},
        metadata: %{reason: :proc_self_stat_unreadable}
      })
    )
  end
end
