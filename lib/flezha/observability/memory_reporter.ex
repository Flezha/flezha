defmodule Flezha.Diagnostics.MemoryReporter do
  @moduledoc """
  Periodic BEAM memory reporter. When enabled, reads the current
  allocator state on a fixed interval and emits one structured
  `[info]` log line per tick.

  Off by default; started by `Flezha.Application` only when the env
  var `FLEZHA_MEMORY_REPORT_INTERVAL_MS` parses as a positive integer
  (the interval in milliseconds between reports).

  Log shape (single logical line, space-separated `key=value` after
  the module marker, so it grep-parses cleanly):

      module=Flezha.Diagnostics.MemoryReporter
        erlang_memory_total=<bytes> erlang_memory_binary=<bytes>
        erlang_memory_processes=<bytes> erlang_memory_ets=<bytes>
        erlang_memory_atom=<bytes> erlang_memory_code=<bytes>
        process_count=<n> port_count=<n>
        binary_alloc_carriers_size=<bytes>
  """

  alias Flezha.Config

  use GenServer

  require Logger

  ## ----- Public API ---------------------------------------------------------

  @doc """
  Returns `{:ok, ms}` when the env var parses as a positive integer,
  `:disabled` otherwise. `Flezha.Application` uses this to decide
  whether to add the reporter to the supervision tree.
  """
  @spec configured_interval_ms() :: {:ok, pos_integer()} | :disabled
  def configured_interval_ms do
    case Config.get(:memory_report_interval_ms) do
      ms when is_integer(ms) and ms > 0 -> {:ok, ms}
      _ -> :disabled
    end
  end

  @doc """
  Builds the supervisor child spec for the reporter. Returns `:ignore`
  when the env var is unset or invalid so the caller can omit the
  child entirely.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec() | :ignore
  def child_spec(opts) do
    case Keyword.get(opts, :interval_ms) || configured_interval_ms() do
      :disabled ->
        :ignore

      {:ok, interval_ms} ->
        build_child_spec(interval_ms)

      interval_ms when is_integer(interval_ms) and interval_ms > 0 ->
        build_child_spec(interval_ms)

      _ ->
        :ignore
    end
  end

  @doc "Capture a single point-in-time memory reading without emitting a log line."
  @spec capture() :: %{required(atom()) => non_neg_integer() | module()}
  def capture do
    mem = :erlang.memory()

    %{
      module: __MODULE__,
      erlang_memory_total: Keyword.get(mem, :total, 0),
      erlang_memory_binary: Keyword.get(mem, :binary, 0),
      erlang_memory_processes: Keyword.get(mem, :processes, 0),
      erlang_memory_ets: Keyword.get(mem, :ets, 0),
      erlang_memory_atom: Keyword.get(mem, :atom, 0),
      erlang_memory_code: Keyword.get(mem, :code, 0),
      process_count: :erlang.system_info(:process_count),
      port_count: :erlang.system_info(:port_count),
      binary_alloc_carriers_size: binary_alloc_carriers_size()
    }
  end

  @doc "Format a captured reading as the single-line `key=value` log payload."
  @spec to_log_line(map()) :: iodata()
  def to_log_line(reading) do
    [
      "module=",
      # `inspect/1` strips the `Elixir.` prefix from module atoms;
      # `Atom.to_string/1` would emit `Elixir.Flezha....` and break the
      # documented grep contract.
      inspect(reading.module),
      " erlang_memory_total=",
      Integer.to_string(reading.erlang_memory_total),
      " erlang_memory_binary=",
      Integer.to_string(reading.erlang_memory_binary),
      " erlang_memory_processes=",
      Integer.to_string(reading.erlang_memory_processes),
      " erlang_memory_ets=",
      Integer.to_string(reading.erlang_memory_ets),
      " erlang_memory_atom=",
      Integer.to_string(reading.erlang_memory_atom),
      " erlang_memory_code=",
      Integer.to_string(reading.erlang_memory_code),
      " process_count=",
      Integer.to_string(reading.process_count),
      " port_count=",
      Integer.to_string(reading.port_count),
      " binary_alloc_carriers_size=",
      Integer.to_string(reading.binary_alloc_carriers_size)
    ]
  end

  ## ----- GenServer ----------------------------------------------------------

  @impl true
  def init(%{interval_ms: interval_ms}) do
    Process.send_after(self(), :report, interval_ms)
    {:ok, %{interval_ms: interval_ms}}
  end

  @impl true
  def handle_info(:report, %{interval_ms: interval_ms} = state) do
    emit_reading()
    Process.send_after(self(), :report, interval_ms)
    {:noreply, state}
  end

  ## ----- internals ----------------------------------------------------------

  defp build_child_spec(interval_ms) do
    %{
      id: __MODULE__,
      start:
        {GenServer, :start_link, [__MODULE__, %{interval_ms: interval_ms}, [name: __MODULE__]]},
      type: :worker,
      restart: :transient
    }
  end

  defp emit_reading do
    # Always-on per-category sample; logged at :info so it reaches Loki.
    Logger.info(to_log_line(capture()))
  end

  # `binary_alloc` carrier size diverges from `:erlang.memory(:binary)`
  # when refc binaries get pinned in carriers without being released to
  # the OS — surfacing it alongside the per-subsystem totals lets an
  # operator distinguish in-use refc binaries from carrier waste.
  defp binary_alloc_carriers_size do
    :recon_alloc.memory(:allocated_types)
    |> Keyword.get(:binary_alloc, 0)
  end
end
