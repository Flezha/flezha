defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  @moduledoc """
  First-party IO-forward subscriber that spawns a heartbeat GenServer to
  keep the remote receiver alive. The process is owned by the
  `Extension.Table` monitor set — if it dies the plugin is disabled.

  Required opts: `:recv_pid`, `:plugin_id`, `:operative_id`, `:namespace`.
  Optional: `:heartbeat_interval` (ms, default 5000).
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.Identificator
  alias Flezha.Service.PluginManager

  use GenServer
  use ProviderBehaviour

  @enforce_keys [:recv_pid, :plugin_id, :operative_id, :namespace]
  defstruct @enforce_keys ++ [:binding_pid, heartbeat_interval: 5000]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:recv_pid, value) when is_binary(value) do
    Identificator.string_to_pid(value)
  end

  defp convert_value(:heartbeat_interval, value) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> {:ok, int_val}
      _ -> {:error, "Invalid value for heartbeat_interval: #{value}"}
    end
  end

  def init_provider(opts) do
    case GenServer.start_link(__MODULE__, opts) do
      {:ok, pid} -> {:ok, Map.put(opts, :binding_pid, pid), pid}
      {:error, reason} -> {:error, reason}
    end
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: msg, metadata: md} = ctx, %{recv_pid: recv_pid}) do
    case Process.send(recv_pid, {:message_forward, {msg, md}}, []) do
      :ok -> {:ok, ctx}
      _ -> {:error, :send_failed}
    end
  end

  def start_link(binding) do
    GenServer.start_link(__MODULE__, binding)
  end

  @impl GenServer
  def init(opts) do
    interval = Map.get(opts, :heartbeat_interval, 5000)
    schedule_heartbeat(interval)

    {:ok,
     %{
       recv_pid: Map.fetch!(opts, :recv_pid),
       plugin_id: Map.fetch!(opts, :plugin_id),
       heartbeat_interval: interval
     }}
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
        Task.start(fn -> PluginManager.disable_plugin(state.plugin_id) end)
        {:stop, :normal, state}
    end
  end

  defp schedule_heartbeat(interval) do
    Process.send_after(self(), :schedule_heartbeat_keepalive, interval)
  end
end
