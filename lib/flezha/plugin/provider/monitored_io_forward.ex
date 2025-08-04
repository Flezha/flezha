defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  alias Flezha.Service.PluginManager
  alias Flezha.Plugin.ComputationBinding
  alias Flezha.Plugin.ComputationPlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  use GenServer

  @enforce_keys [:recv_pid] ++ ComputationPlugin.required_computation_keys()
  defstruct @enforce_keys ++
              [:binding_pid, heartbeat_interval: 5000] ++
              ComputationPlugin.optional_computation_keys()

  @impl PluginBase
  def plugin_id(), do: "monitored_io_forward"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.MonitoredIOForward do
    use ComputationPlugin
  end

  defimpl ComputationBinding, for: Flezha.Plugin.Provider.MonitoredIOForward do
    @impl ComputationBinding
    def bind(settings) do
      {:ok, pid} = GenServer.start_link(Flezha.Plugin.Provider.MonitoredIOForward, settings)

      {:ok,
       %Flezha.Plugin.Provider.MonitoredIOForward{
         settings
         | binding_pid: pid
       }}
    end

    @impl true
    def exec(settings, payload) do
      case Process.send(settings.recv_pid, {:message_forward, payload}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, :send_failed}
      end
    end
  end

  def start_link(settings) do
    GenServer.start_link(__MODULE__, settings)
  end

  @impl GenServer
  def init(settings) do
    schedule_heartbeat(settings.heartbeat_interval)

    {:ok, settings}
  end

  @impl GenServer
  def handle_info(:shutdown_signal, state) do
    {:stop, :normal, state}
  end

  @impl GenServer
  def handle_info(:schedule_heartbeat_keepalive, state) do
    schedule_heartbeat(state.heartbeat_interval)

    Process.send(state.recv_pid, {:heartbeat_keepalive, self()}, [])

    receive do
      {:heartbeat_keepalive, :ok} ->
        {:noreply, state}
    after
      state.heartbeat_interval ->
        Task.start(fn -> PluginManager.disable_plugin(state) end)

        {:stop, :normal, state}
    end
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :schedule_heartbeat_keepalive, interval)
  end
end
