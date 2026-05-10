defmodule Flezha.Shutdown.SignalHandler do
  @moduledoc """
  Installs OS signal handlers (SIGTERM/SIGINT) so the controller
  enters graceful shutdown in every BEAM run mode (release, mix,
  iex, escript). Started **first** in the application supervision
  tree so the handler is in place before any other service can
  observe a delivered signal.

  Signal escalation:
    1. First SIGTERM/SIGINT — invoke
       `Flezha.Shutdown.Coordinator.shutdown(reason)` then
       `:init.stop()`.
    2. Second signal during shutdown — log + ignore.
    3. Third signal — `:erlang.halt(143)` escape hatch.

  When `:flezha, :graceful_shutdown_enabled` is `false`, this server
  still starts but does NOT install OS handlers — the default OTP
  signal behaviour (immediate BEAM termination on SIGTERM) applies.

  If `:os.set_signal/2` raises (unsupported OS, sandboxing) the
  failure is caught and the coordinator is told
  `report_signal_handler_status(:degraded)` so `flz controller status`
  can surface the condition.
  """

  use GenServer
  require Logger

  alias Flezha.Shutdown.Coordinator

  defstruct count: 0, enabled: true, status: :ok

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 5_000,
      type: :worker
    }
  end

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc false
  def signal_count() do
    GenServer.call(__MODULE__, :signal_count)
  end

  @impl true
  def init(_) do
    enabled = Application.get_env(:flezha, :graceful_shutdown_enabled, true)
    {:ok, %__MODULE__{enabled: enabled}, {:continue, :install_signal_handler}}
  end

  @impl true
  def handle_continue(:install_signal_handler, %__MODULE__{enabled: false} = state) do
    Logger.info(
      "[shutdown] graceful_shutdown_enabled=false; OS signal handlers NOT installed"
    )

    {:noreply, state}
  end

  def handle_continue(:install_signal_handler, %__MODULE__{enabled: true} = state) do
    state =
      try do
        :os.set_signal(:sigterm, :handle)
        :os.set_signal(:sigint, :handle)
        Logger.info("[shutdown] OS signal handlers installed (SIGTERM, SIGINT)")
        %{state | status: :ok}
      catch
        kind, reason ->
          Logger.warning(
            "[shutdown] os.set_signal unavailable: #{inspect({kind, reason})}; " <>
              "falling back to release-mode SIGTERM"
          )

          %{state | status: :degraded}
      end

    safe_report_status(state.status)
    {:noreply, state}
  end

  @impl true
  def handle_info(:sigterm, state), do: handle_signal(:sigterm, state)
  def handle_info(:sigint, state), do: handle_signal(:sigint, state)

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call(:signal_count, _from, state) do
    {:reply, state.count, state}
  end

  defp handle_signal(signal, %__MODULE__{count: 0} = state) do
    Logger.info("[shutdown] received #{signal}")
    safe_emit_requested(signal)

    Task.start(fn ->
      try do
        Coordinator.shutdown(signal)
      catch
        :exit, reason ->
          Logger.warning("[shutdown] coordinator exit during signal handling: #{inspect(reason)}")
      end

      :init.stop()
    end)

    {:noreply, %{state | count: 1}}
  end

  defp handle_signal(signal, %__MODULE__{count: 1} = state) do
    Logger.info("[shutdown] already in progress, ignoring #{signal}")
    {:noreply, %{state | count: 2}}
  end

  defp handle_signal(signal, %__MODULE__{count: c} = state) when c >= 2 do
    Logger.warning("[shutdown] forced halt requested via third #{signal}")
    :erlang.halt(143)
    {:noreply, state}
  end

  defp safe_report_status(status) do
    try do
      Coordinator.report_signal_handler_status(status)
    catch
      :exit, _ -> :ok
    end
  end

  defp safe_emit_requested(signal) do
    try do
      :telemetry.execute([:flezha, :shutdown, :requested], %{}, %{
        reason: signal,
        timestamp_ms: System.system_time(:millisecond)
      })
    rescue
      _ -> :ok
    end
  end
end
