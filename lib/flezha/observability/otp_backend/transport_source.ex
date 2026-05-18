defmodule Flezha.Observability.OtpBackend.TransportSource do
  @moduledoc """
  Polled source emitting per-transport (`:uds`, `:quic`) metrics.

  Reads the public `Flezha.Transport.Server.list_active_transports/0`
  + `get_stats/1` surface. Emits `:open_connections` per transport (a
  gauge sourced from the server's pending-stream count, which is the
  closest Elixir-side proxy until the Rust NIF surfaces a true
  per-transport open-connection count). Bytes in/out are sourced from
  the same accessor when the NIF starts surfacing them — until then
  the source emits only what it can read accurately.

  Cardinality: one metadata shape per active transport (≤ 2 today),
  so the budget is a small constant.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :flezha_transport`.
  """

  use GenServer

  alias Flezha.Observability.{Bus, Registry, Signal}
  alias Flezha.Transport.Server, as: TransportServer

  @source :flezha_transport
  @default_poll_ms 1_000

  # ---------- GenServer ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    Registry.register_source(%{
      name: @source,
      kinds: [:metric],
      flavour: :process_backed,
      cardinality_budget: 20
    })

    interval = poll_interval_ms()
    Process.send_after(self(), :poll, interval)
    {:ok, %{interval: interval}}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    poll_transports()
    Process.send_after(self(), :poll, state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Internals ----------

  defp poll_transports() do
    transports =
      try do
        TransportServer.list_active_transports()
      catch
        _, _ -> []
      end

    Enum.each(transports, &emit_for_transport/1)
  end

  defp emit_for_transport(transport) do
    case TransportServer.get_stats(transport) do
      {:ok, %{pending_streams: pending}} ->
        emit(:open_connections, %{value: pending}, %{transport: transport})

      _ ->
        :ok
    end
  end

  defp emit(name, measurements, metadata) do
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

  defp poll_interval_ms() do
    Application.get_env(:flezha, :observability, [])
    |> Keyword.get(:otp_backend, [])
    |> Keyword.get(:poll_intervals, %{})
    |> Map.get(@source, @default_poll_ms)
  end
end
