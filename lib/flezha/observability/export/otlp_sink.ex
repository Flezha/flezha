defmodule Flezha.Observability.Export.OtlpSink do
  @moduledoc """
  Sink that batches signals and POSTs them as OTLP/HTTP protobuf
  bodies via `OtlpClient`. Overflow policy is `:block` — operators
  who turn this on accept producer back-pressure rather than silent
  loss. Mailbox high-water default `10_000`.

  Flush triggers (whichever fires first):
    * Buffer reaches `:otlp_batch_max` (default `512`).
    * `:otlp_flush_interval_ms` (default `10_000`) elapses with at
      least one buffered signal.

  Emits `:otlp_batch_flushed` (with `batch_size`, `duration_us`) on
  success; emits `:otlp_batch_dropped` (with `batch_size`, `attempts`)
  when retries are exhausted.
  """

  use GenServer

  @behaviour Flezha.Observability.Sink

  alias Flezha.Config.Backoff
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.Export.{OtlpClient, OtlpCodec}
  alias Flezha.Observability.Registry, as: ObsRegistry

  @sink_name :otlp
  @default_high_watermark 10_000

  # ---------- Sink behaviour ----------

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  # ---------- GenServer ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    config = Application.get_env(:flezha, :observability_export, [])
    flush_ms = Keyword.get(config, :otlp_flush_interval_ms, 10_000)
    batch_max = Keyword.get(config, :otlp_batch_max, 512)

    {:ok, _} = register_self()

    Process.send_after(self(), :flush_tick, flush_ms)

    {:ok,
     %{
       buffer: [],
       size: 0,
       flush_ms: flush_ms,
       batch_max: batch_max,
       dropped: 0
     }}
  end

  @impl GenServer
  def handle_call({:signal, signal}, _from, state) do
    state = ingest(state, signal)
    {:reply, :ok, state}
  end

  def handle_call(:stats, _from, state) do
    {:reply, %{buffered: state.size, dropped: state.dropped}, state}
  end

  @impl GenServer
  def handle_info({:signal, signal}, state) do
    state = ingest(state, signal)
    {:noreply, state}
  end

  def handle_info(:flush_tick, state) do
    state = maybe_flush(state, :time)
    Process.send_after(self(), :flush_tick, state.flush_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Internals ----------

  # Drop sink-internal events so the exporter cannot feedback-loop on
  # its own success/failure announcements.
  defp ingest(state, %Signal{source: :observability_export}), do: state

  defp ingest(state, signal) do
    state = append(state, signal)
    maybe_flush(state, :size)
  end

  defp append(state, signal) do
    %{state | buffer: [signal | state.buffer], size: state.size + 1}
  end

  defp maybe_flush(%{size: 0} = state, _), do: state

  defp maybe_flush(state, :size) do
    if state.size >= state.batch_max, do: flush(state), else: state
  end

  defp maybe_flush(state, :time), do: flush(state)

  defp flush(%{buffer: buffer} = state) do
    signals = Enum.reverse(buffer)
    encoded = OtlpCodec.encode_batch(signals)

    start = System.monotonic_time(:microsecond)
    results = send_each(encoded)
    duration_us = System.monotonic_time(:microsecond) - start

    handle_results(results, state.size, duration_us)
    %{state | buffer: [], size: 0}
  end

  defp send_each(%{metrics: m, logs: l, traces: t}) do
    [
      maybe_post(:metrics, m),
      maybe_post(:logs, l),
      maybe_post(:traces, t)
    ]
    |> Enum.reject(&is_nil/1)
  end

  defp maybe_post(_kind, nil), do: nil
  defp maybe_post(kind, body), do: {kind, OtlpClient.post(kind, body)}

  defp handle_results([], _size, _duration_us), do: :ok

  defp handle_results(results, size, duration_us) do
    failures = Enum.filter(results, fn {_kind, r} -> match?({:error, _}, r) end)

    if failures == [] do
      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: :observability_export,
          name: :otlp_batch_flushed,
          measurements: %{batch_size: size, duration_us: duration_us}
        })
      )
    else
      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: :observability_export,
          name: :otlp_batch_dropped,
          measurements: %{batch_size: size, attempts: Backoff.ladder(:otlp_post).give_up},
          metadata: %{
            failures: Enum.map(failures, fn {kind, {:error, r}} -> {kind, r} end) |> inspect()
          }
        })
      )
    end
  end

  defp register_self() do
    pid = self()

    :ok =
      ObsRegistry.register_external_sink(%{
        name: @sink_name,
        module: __MODULE__,
        config: %{},
        filter: filter(),
        overflow: :block,
        mailbox_high_watermark: @default_high_watermark,
        block_timeout_ms: 30_000,
        pid: pid
      })

    {:ok, pid}
  end
end
