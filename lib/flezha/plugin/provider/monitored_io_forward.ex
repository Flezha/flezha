defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  @moduledoc """
  First-party IO-forward subscriber whose declared worker is a heartbeat
  GenServer that keeps the remote receiver alive. The table starts it at
  activation and monitors it — if it dies the plugin is disabled.

  Required opts: `:recv_pid`, `:plugin_id`, `:operator_id`, `:namespace`.
  Optional: `:heartbeat_interval` (ms, default 5000).
  """

  alias Flezha.Partition
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.PluginManager

  use GenServer
  use Flezha.Process.Timer, keys: [:heartbeat]

  use ProviderBehaviour,
    options: [
      recv_pid: [type: :pid, required: true, doc: "The BEAM process the payload is sent to"],
      plugin_id: [
        type: :string,
        required: true,
        doc: "The plugin id the heartbeat detaches, with every member a group fanned it out to"
      ],
      operator_id: [type: :string, required: true, doc: "The operator this binding belongs to"],
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

  # A part or a notice never suspends the Overwatch on a busy distribution
  # link: it is dropped instead, which a preview allows and an output does not.
  @impl true
  def on_preview(%Ctx.IO{message: msg, metadata: md} = ctx, %{recv_pid: recv_pid}) do
    _ = Process.send(recv_pid, {:preview_forward, {msg, md}}, [:nosuspend])
    {:ok, ctx}
  end

  # The table calls this on an unsubscribe, a teardown and a restart alike; the
  # receiver asks the registry which it was.
  @impl true
  def terminate_provider(%{recv_pid: recv_pid, plugin_id: plugin_id}) do
    send(recv_pid, {:listener_released, plugin_id})
    :ok
  end

  # A group fans one plugin out to every member under one `plugin_id`. Keyed by
  # that alone the members would share one heartbeat, and the first member's
  # disable would stop it under the others.
  @impl true
  def worker_specs(%{plugin_id: plugin_id, partition_index: index} = opts),
    do: [{Partition.member_id(plugin_id, index), opts}]

  def worker_specs(%{plugin_id: plugin_id} = opts), do: [{plugin_id, opts}]

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
       operator_id: Map.fetch!(opts, :operator_id),
       namespace: Map.fetch!(opts, :namespace),
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
        Task.start(fn ->
          PluginManager.detach(state.namespace, state.operator_id, state.plugin_id)
        end)

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
