defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  alias Flezha.Service.PluginManager
  alias Flezha.Processor.IOBinding
  alias Flezha.Plugin.ProviderBehaviour

  use GenServer
  use ProviderBehaviour

  @enforce_keys [:recv_pid, :plugin_id, :processor_id, :namespace]
  defstruct @enforce_keys ++ [:binding_pid, heartbeat_interval: 5000]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:recv_pid, value) when is_binary(value) do
    case String.to_existing_atom(value) do
      pid when is_atom(pid) ->
        case Process.whereis(pid) do
          nil -> {:error, "Process #{value} not found"}
          pid -> pid
        end

      _ ->
        {:error, "Invalid PID format"}
    end
  end

  defp convert_value(:heartbeat_interval, value) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> int_val
      _ -> 5000
    end
  end

  defimpl IOBinding, for: Flezha.Plugin.Provider.MonitoredIOForward do
    @impl true
    def bind(binding) do
      {:ok, pid} = GenServer.start_link(Flezha.Plugin.Provider.MonitoredIOForward, binding)

      binding = %Flezha.Plugin.Provider.MonitoredIOForward{
        binding
        | binding_pid: pid
      }

      {:ok, {binding, pid}}
    end

    @impl true
    def exec(binding, payload) do
      case Process.send(binding.recv_pid, {:message_forward, payload}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, :send_failed}
      end
    end
  end

  def start_link(binding) do
    GenServer.start_link(__MODULE__, binding)
  end

  @impl GenServer
  def init(binding) do
    schedule_heartbeat(binding.heartbeat_interval)

    {:ok, binding}
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
        Task.start(fn ->
          PluginManager.disable_plugin(state.plugin_id)
        end)

        {:stop, :normal, state}
    end
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :schedule_heartbeat_keepalive, interval)
  end
end
