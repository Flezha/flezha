defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  @moduledoc """
  First-party IO-forward subscriber whose declared worker is a heartbeat
  GenServer that keeps the remote receiver alive. The table starts it at
  activation and monitors it — if it dies the plugin is disabled.

  Required opts: `:recv_pid`, `:plugin_id`, `:operative_id`, `:namespace`.
  Optional: `:heartbeat_interval` (ms, default 5000).
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.PluginManager

  use GenServer
  use Flezha.Process.Timer, keys: [:heartbeat]

  use ProviderBehaviour,
    options: [
      recv_pid: [type: :pid, required: true, doc: "The BEAM process the payload is sent to"],
      plugin_id: [type: :string, required: true, doc: "The plugin id the heartbeat disables"],
      operative_id: [type: :string, required: true, doc: "The operative this binding belongs to"],
      namespace: [type: :any, required: true, doc: "The namespace this binding belongs to"],
      heartbeat_interval: [
        type: :int,
        default: 5000,
        min: 1,
        doc: "Milliseconds between keepalives, and the wait for each reply"
      ]
    ],
    worker: [module: __MODULE__, restart: :temporary]

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: msg, metadata: md} = ctx, %{recv_pid: recv_pid}) do
    case Process.send(recv_pid, {:message_forward, {msg, md}}, []) do
      :ok -> {:ok, ctx}
      _ -> {:error, :send_failed}
    end
  end

  @doc "The heartbeat process. Started by the extension table at activation."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts) do
    GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))
  end

  @impl GenServer
  def init(opts) do
    interval = Map.fetch!(opts, :heartbeat_interval)

    {:ok,
     arm_heartbeat(%{
       recv_pid: Map.fetch!(opts, :recv_pid),
       plugin_id: Map.fetch!(opts, :plugin_id),
       heartbeat_interval: interval
     })}
  end

  @impl GenServer
  def handle_info(:shutdown_signal, state) do
    {:stop, :normal, state}
  end

  @impl GenServer
  def handle_info(:schedule_heartbeat_keepalive, state) do
    state = arm_heartbeat(state)

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

  defp arm_heartbeat(state) do
    arm_timer(
      state,
      :heartbeat,
      state.heartbeat_interval,
      :schedule_heartbeat_keepalive
    )
  end
end
