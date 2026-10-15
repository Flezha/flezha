defmodule Flezha.Plugin.Provider.MonitoredIOForward do
  @moduledoc """
  First-party IO-forward subscriber whose declared worker is a heartbeat
  GenServer that keeps the remote receiver alive. The table starts it at
  activation and monitors it — if it dies the plugin is disabled.

  In `tally` mode it forwards nothing: each output adds one to a `:counters`
  slot for its channel, created by `init_provider/1` in the relay's Overwatch so
  the counting stays on the relay's node, and the heartbeat worker reports the
  totals every `tally_ms` as `{:tally_report, member, epoch, totals}`. `epoch`
  is new each time the plugin is prepared, so a restarted count is told apart.

  Required opts: `:recv_pid`, `:plugin_id`, `:relay_id`, `:namespace`.
  Optional: `:heartbeat_interval` (ms, default 5000), `:mode`, `:channels`,
  `:tally_ms`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Process.Watch
  alias Flezha.Service.PluginManager

  use GenServer
  use Flezha.Process.Timer, keys: [:heartbeat, :tally]

  use ProviderBehaviour,
    options: [
      recv_pid: [type: :pid, required: true, doc: "The BEAM process the payload is sent to"],
      plugin_id: [
        type: :string,
        required: true,
        doc: "The plugin id the heartbeat detaches, with every member a group fanned it out to"
      ],
      relay_id: [type: :string, required: true, doc: "The relay this binding belongs to"],
      namespace: [type: :string, required: true, doc: "The namespace this binding belongs to"],
      heartbeat_interval: [
        type: :int,
        default: 5000,
        min: 1,
        doc: "Milliseconds between keepalives, and the wait for each reply"
      ],
      mode: [
        type: :enum,
        values: ["forward", "tally"],
        default: "forward",
        doc: "`tally` counts outputs per channel and reports the totals instead of forwarding"
      ],
      channels: [
        type: :names,
        default: [],
        doc: "The channels a tally counts by name; any other counts under the empty name"
      ],
      tally_ms: [
        type: :int,
        default: 1000,
        min: 100,
        doc: "Milliseconds between a tally's reports"
      ],
      # The counters `init_provider/1` creates for a tally.
      tally: [type: :any, internal: true]
    ],
    worker: [module: __MODULE__, restart: :temporary]

  # Random rather than a VM-unique integer, which another node may mint again
  # after a move; inside the range a JSON reader holds exactly.
  @max_epoch 9_007_199_254_740_991

  @impl true
  def init_provider(%{mode: "tally", channels: channels} = opts) do
    other = length(channels) + 1

    {:ok,
     Map.put(opts, :tally, %{
       counters: :counters.new(other, [:write_concurrency]),
       index: Map.new(Enum.with_index(channels, 1)),
       other: other,
       epoch: :rand.uniform(@max_epoch)
     })}
  end

  def init_provider(opts), do: {:ok, opts}

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{metadata: md} = ctx, %{tally: %{counters: counters} = tally}) do
    :counters.add(counters, Map.get(tally.index, md["channel"], tally.other), 1)
    {:ok, ctx}
  end

  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  @impl true
  def on_call(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

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
  # disable would stop it under the others. A tally's worker is keyed by its
  # epoch too: a live worker under the same key is adopted, and one left by an
  # Overwatch that died reports counters nothing adds to any more.
  @impl true
  def worker_specs(%{tally: %{epoch: epoch}} = opts), do: [{{member_key(opts), epoch}, opts}]
  def worker_specs(opts), do: [{member_key(opts), opts}]

  defp member_key(%{partition_index: index} = opts), do: {WorkerSup.key(opts), index}
  defp member_key(opts), do: WorkerSup.key(opts)

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

    state = %{
      recv_pid: Map.fetch!(opts, :recv_pid),
      plugin_id: Map.fetch!(opts, :plugin_id),
      relay_id: Map.fetch!(opts, :relay_id),
      namespace: Map.fetch!(opts, :namespace),
      heartbeat_interval: interval,
      member: Map.get(opts, :source_relay_id),
      tally: Map.get(opts, :tally),
      tally_ms: Map.get(opts, :tally_ms),
      counted_by: counted_by(opts)
    }

    {:ok, state |> arm_heartbeat() |> arm_tally()}
  end

  # A tally's counters live in the Overwatch that prepared them, so its worker
  # goes when that Overwatch does; the next one prepares a worker of its own.
  defp counted_by(%{tally: %{}, overwatcher_pid: pid}) when is_pid(pid),
    do: Watch.watch(Watch.new(), pid, :overwatch)

  defp counted_by(_opts), do: Watch.new()

  @impl GenServer
  def handle_info(:shutdown_signal, state) do
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(state.counted_by, ref) do
      {:unknown, _} -> {:noreply, state}
      {[:overwatch], _} -> {:stop, :normal, state}
    end
  end

  def handle_info(:tally_report, %{tally: tally} = state) do
    totals =
      for {channel, slot} <- tally.index,
          into: %{"" => :counters.get(tally.counters, tally.other)},
          do: {channel, :counters.get(tally.counters, slot)}

    send(state.recv_pid, {:tally_report, state.member, tally.epoch, totals})
    {:noreply, arm_tally(state)}
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
          PluginManager.detach(state.namespace, state.relay_id, state.plugin_id)
        end)

        {:stop, :normal, state}
    end
  end

  defp arm_tally(%{tally: nil} = state), do: state
  defp arm_tally(state), do: arm_timer(state, :tally, state.tally_ms, :tally_report)

  defp arm_heartbeat(state) do
    arm_timer(
      state,
      :heartbeat,
      state.heartbeat_interval,
      :schedule_heartbeat_keepalive
    )
  end
end
