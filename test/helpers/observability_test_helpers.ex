defmodule Flezha.Observability.TestSinks do
  @moduledoc false
  # Sink implementations used only by tests under
  # `test/flezha/observability/` and `test/flezha/service/`.

  @doc """
  Return once `sink` has handled every signal already sent to it: a signal is
  a plain message, so a system call queued behind it answers after it.
  `sink` is a registered sink name, a registered process name, or a pid.
  """
  def settle(pid) when is_pid(pid) do
    _ = :sys.get_state(pid)
    :ok
  end

  def settle(name) when is_atom(name) do
    case Flezha.Observability.Registry.lookup_sink(name) do
      {:ok, %{pid: pid}} when is_pid(pid) -> settle(pid)
      _ -> settle(Process.whereis(name) || raise("no sink or process named #{inspect(name)}"))
    end
  end

  @doc """
  Turn the PubSub sink on for the calling test, and back off when it ends.
  `config/test.exs` sets `:pubsub_sink_enabled` false and starts no sink, and
  every tail subscription gate reads both.
  """
  def claim_pubsub_sink do
    prev = Application.get_env(:flezha, :observability, [])
    Application.put_env(:flezha, :observability, Keyword.put(prev, :pubsub_sink_enabled, true))
    ExUnit.Callbacks.on_exit(fn -> Application.put_env(:flezha, :observability, prev) end)

    if Process.whereis(Flezha.Observability.PubSubSink) == nil do
      sink = :"obs_tail_pubsub_#{:erlang.unique_integer([:positive])}"
      sink_spec = %{name: sink, module: Flezha.Observability.PubSubSink, config: %{}}
      {:ok, _pid} = Flezha.Observability.Registry.register_sink(sink_spec)
      ExUnit.Callbacks.on_exit(fn -> Flezha.Observability.Registry.unregister_sink(sink) end)
    end

    :ok
  end

  @doc "The processes subscribed to `source`'s live signals."
  def subscribers(source) do
    topic = Flezha.Observability.PubSubSink.topic_for(source)
    Elixir.Registry.lookup(Flezha.Observability.PubSub, topic)
  end

  # Everything that starts after the registry, in its boot order.
  @after_registry [
    Flezha.Observability.SinkSupervisor,
    Flezha.Observability.LoggerHandler,
    Flezha.Observability.TelemetryBridge,
    Flezha.Observability.DefaultSinks,
    Flezha.Observability.OtpBackend.Supervisor,
    Flezha.Observability.Export.Supervisor,
    Flezha.Observability.AuditLog.Supervisor
  ]

  @doc """
  Start the observability registry again, if a test stopped it, and everything
  after it: a fresh registry holds no sink and no bridge source.
  """
  def restart_after_registry() do
    sup = Flezha.Observability.Supervisor
    _ = Supervisor.restart_child(sup, Flezha.Observability.Registry)

    for child <- Enum.reverse(@after_registry),
        child != Flezha.Observability.DefaultSinks,
        do: :ok = Supervisor.terminate_child(sup, child)

    for child <- @after_registry, do: {:ok, _} = start_again(sup, child)
    :ok
  end

  # A `:temporary` one-shot leaves no child behind once it ran, so it is started anew.
  defp start_again(sup, Flezha.Observability.DefaultSinks),
    do: Supervisor.start_child(sup, Flezha.Observability.DefaultSinks)

  defp start_again(sup, child), do: Supervisor.restart_child(sup, child)

  @doc """
  Wait for a signal the `Counting` sink forwarded to the calling process that
  satisfies `pred`, or `nil` once `timeout` ms have elapsed. Signals that do
  not satisfy `pred` are consumed — an `:any/:any` sink also receives the
  traffic of neighbouring tests.
  """
  def receive_signal(pred, timeout \\ 500) do
    await_signal(pred, System.monotonic_time(:millisecond) + timeout)
  end

  defp await_signal(pred, deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining <= 0 ->
        nil

      remaining ->
        receive do
          {:counted, %Flezha.Observability.Signal{} = signal} ->
            if pred.(signal), do: signal, else: await_signal(pred, deadline)
        after
          remaining -> nil
        end
    end
  end
end

defmodule Flezha.Observability.TestSinks.Counting do
  @moduledoc false
  # Counts every received signal. Each instance owns a `:counters`
  # array passed via config so multiple counting sinks can co-exist.
  #
  # The bus-level `filter/0` is `:any/:any` so the sink receives
  # every signal. The handler ignores signals whose `:source` is
  # not in the per-instance `config.sources` allowlist when one is
  # provided — keeps the test process's mailbox from filling up with
  # unrelated signals (telemetry bridge, logger handler, neighbouring
  # tests).

  use GenServer

  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{counter_ref: :term, parent: :term}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_info(
        {:signal, %{source: s} = signal},
        %{config: %{counter_ref: ref, parent: parent} = config} = state
      ) do
    if accept_source?(config, s) do
      :counters.add(ref, 1, 1)
      if is_pid(parent), do: send(parent, {:counted, signal})
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp accept_source?(%{sources: list}, source) when is_list(list), do: source in list
  defp accept_source?(_, _), do: true
end

defmodule Flezha.Observability.TestSinks.Slow do
  @moduledoc false
  # Parks on its first signal (until sent `:unpark`) to deliberately back the
  # mailbox up.

  use GenServer

  @behaviour Flezha.Observability.Sink

  alias FlezhaTest.Helper.NodeTestHelper

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_info({:signal, _signal}, state) do
    NodeTestHelper.park()
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end

defmodule Flezha.Observability.TestSinks.Blocking do
  @moduledoc false
  # Verifies the `:block` overflow policy. Handles
  # `{:signal, signal}` via GenServer.call.

  use GenServer

  @behaviour Flezha.Observability.Sink

  @impl Flezha.Observability.Sink
  def config_schema(), do: %{parent: :term}

  @impl Flezha.Observability.Sink
  def filter(), do: %{kinds: :any, sources: :any}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts), do: {:ok, opts}

  @impl true
  def handle_call({:signal, signal}, from, %{config: config} = state) do
    parent = Map.get(config, :parent)
    if is_pid(parent), do: send(parent, {:blocked_received, signal})

    # `hold: <signal name>` keeps that one signal's caller waiting until the
    # sink is sent `:release`, so a test can observe what else the emitting
    # process is holding up meanwhile.
    if Map.get(config, :hold) == signal.name do
      {:noreply, Map.put(state, :held, from)}
    else
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info(:release, state) do
    case Map.pop(state, :held) do
      {nil, state} ->
        {:noreply, state}

      {from, state} ->
        GenServer.reply(from, :ok)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}
end
