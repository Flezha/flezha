defmodule FlezhaTest.Plugin.SharedInjectionTest do
  @moduledoc """
  The record path of a shared source: its one worker injects each record
  straight into every bound relay, without waiting on any of them. A
  relay whose input is durable runs the source on its own and confirms
  every record, so a slow one holds only its own upstream; a poller of its own
  advances its content only once the relay took it.

  The test process stands in for a relay: it holds the extension table, so
  every injection lands in its own mailbox.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import FlezhaTest.HttpPollCase, only: [start_origin: 1, set: 2, conditional_requests: 1]
  import FlezhaTest.Helper.ProviderPrepare, only: [take_ingest: 2]

  import ExUnit.CaptureLog

  alias Flezha.Cluster.Directory
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.HttpPoll
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.Shared
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebsocketServer

  @namespace "shared_injection_ns"

  # A shareable source written against the old contract: its worker injects
  # into `overwatcher_pid`, the copy, once in each mode.
  defmodule LegacyWorker do
    use GenServer

    alias Flezha.Plugin.Host

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: opts.name)

    @impl true
    def init(opts) do
      send(self(), :feed)
      {:ok, opts}
    end

    @impl true
    def handle_info(:feed, opts) do
      Host.inject(opts.overwatcher_pid, "old", %{}, :fire_and_forget)
      send(opts.test, {:verdict, Host.inject(opts.overwatcher_pid, "old", %{}, :verdict, 1_000)})
      {:noreply, opts}
    end
  end

  defmodule LegacySource do
    use Flezha.Plugin.ProviderBehaviour,
      feeds: true,
      shareable: true,
      options: [test: [type: :pid, doc: "The test the worker reports to"]],
      worker: [module: FlezhaTest.Plugin.SharedInjectionTest.LegacyWorker, restart: :temporary]

    @impl true
    def worker_specs(opts), do: [{opts.source_plugin_id, opts}]
  end

  # What `Relay.Plugins.env/1` says of an effectively-once relay.
  @effectively_once %{effectively_once: true, durable_input: true}

  defp websocket(server, name) do
    %Plugin{
      id: name,
      provider: Websocket,
      namespace: @namespace,
      relay_id: "op",
      opts: %{uri: "ws://127.0.0.1:#{server.port}/feed", channel: "input"}
    }
  end

  defp poll(origin, name) do
    %Plugin{
      id: name,
      provider: HttpPoll,
      namespace: @namespace,
      relay_id: "op",
      opts: %{
        url: "http://127.0.0.1:#{origin.port}/config",
        channel: "config_in",
        interval_ms: 1_000,
        timeout_ms: 500
      }
    }
  end

  # Bind `plugin` on a relay table the calling process holds.
  defp bind(plugin, env) do
    {:ok, table} =
      Overwatch.__extensions_catalog__()
      |> Table.new(shared: Shared)
      |> Table.bulk_subscribe([{plugin.id, plugin}])

    {table, []} = Table.bulk_activate(table, env)
    table
  end

  # A second relay bound to `plugin`: it answers every injection with
  # `verdict` and reports it to the test as `{:other, kind, message}`.
  defp other_relay(plugin, env, verdict) do
    test = self()

    pid =
      spawn_link(fn ->
        table = bind(plugin, env)
        send(test, {:bound, self()})
        forward(test, table, verdict)
      end)

    assert_receive {:bound, ^pid}, 5_000
    pid
  end

  defp forward(test, table, verdict) do
    receive do
      {:"$gen_call", from, {kind, message, _metadata}} ->
        send(test, {:other, kind, message})
        GenServer.reply(from, verdict)
        forward(test, table, verdict)

      {:"$gen_cast", {:io_in, message, _metadata}} ->
        send(test, {:other, :cast, message})
        forward(test, table, verdict)

      {:unbind, from} ->
        Table.teardown_all(table, :now)
        send(from, :unbound)
    end
  end

  defp unbind(pid) do
    send(pid, {:unbind, self()})
    assert_receive :unbound, 5_000
  end

  defp confirm_frame do
    receive do
      {:"$gen_call", from, {:io_in, message, metadata}} ->
        GenServer.reply(from, :ok)
        {message, metadata}
    after
      5_000 -> flunk("no confirming injection")
    end
  end

  defp cast_frame do
    receive do
      {:"$gen_cast", {:io_in, message, metadata}} -> {message, metadata}
    after
      5_000 -> flunk("no cast injection")
    end
  end

  defp free_port do
    {:ok, listen} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(listen)
    :gen_tcp.close(listen)
    port
  end

  describe "a shared websocket" do
    setup do
      %{server: WebsocketServer.start()}
    end

    test "confirms every frame for an effectively-once relay and drops none",
         %{server: server} do
      table = bind(websocket(server, "eo_feed"), @effectively_once)
      assert_receive {:ws_open, _}, 5_000

      for n <- 1..10, do: WebsocketServer.broadcast(server, "f#{n}")

      assert Enum.map(1..10, fn _ -> elem(confirm_frame(), 0) end) ==
               Enum.map(1..10, &"f#{&1}")

      refute Map.has_key?(Table.workers(table, "eo_feed"), :shared)
      Table.teardown_all(table, :now)
    end

    test "an effectively-once relay runs a connection of its own beside the shared one",
         %{server: server} do
      plugin = websocket(server, "split_feed")
      plain = other_relay(plugin, %{}, :ok)
      assert_receive {:ws_open, _}, 5_000
      table = bind(plugin, @effectively_once)
      assert_receive {:ws_open, _}, 5_000
      assert WebsocketServer.connections(server) == 2

      # The effectively-once relay answers nothing: its own socket waits on
      # it, and the plain relay's frames keep coming.
      for n <- 1..5, do: WebsocketServer.broadcast(server, "w#{n}")
      for _ <- 1..5, do: assert_receive({:other, :cast, "w" <> _}, 5_000)

      unbind(plain)
      Table.teardown_all(table, :now)
    end

    test "casts to each bound relay, stamped with its channel", %{server: server} do
      plugin = websocket(server, "mixed_feed")
      table = bind(plugin, %{})
      assert_receive {:ws_open, _}, 5_000
      other = other_relay(put_in(plugin.opts.channel, "other_in"), %{}, :ok)

      WebsocketServer.broadcast(server, "both")

      assert {"both", %{"channel" => "input"}} = cast_frame()
      assert_receive {:other, :cast, "both"}, 5_000
      assert WebsocketServer.connections(server) == 1

      unbind(other)
      WebsocketServer.broadcast(server, "one")
      assert {"one", _} = cast_frame()
      refute_receive {:other, _, _}, 200
      Table.teardown_all(table, :now)
    end
  end

  test "relays bound while the socket cannot connect get frames once it does" do
    log = capture_log(fn -> late_frames(free_port()) end)
    assert log =~ "Connecting to host failed"
  end

  defp late_frames(port) do
    plugin = websocket(%{port: port}, "late_feed")
    table = bind(plugin, %{})
    other = other_relay(plugin, %{}, :ok)

    # The socket has taken both retargets, while it could not connect.
    {:disconnected, _data} = :sys.get_state(WorkerSup.worker_pid(Websocket, "late_feed"))

    server = WebsocketServer.start(port)
    assert_receive {:ws_open, _}, 10_000
    WebsocketServer.broadcast(server, "late")

    assert {"late", _} = cast_frame()
    assert_receive {:other, :cast, "late"}, 5_000

    unbind(other)
    Table.teardown_all(table, :now)
  end

  test "a source injecting into its copy loses the record, counted, and is warned of once" do
    start_supervised!({WorkerSup, name: WorkerSup.family(LegacySource)})
    on_exit(NodeTestHelper.override_log_level(:warning))

    plugin = %Plugin{
      id: "legacy",
      provider: LegacySource,
      namespace: @namespace,
      relay_id: "op",
      opts: %{test: self()}
    }

    log =
      capture_log(fn ->
        table = bind(plugin, %{})
        assert_receive {:verdict, {:error, :not_a_target}}, 5_000
        [copy] = Directory.plugin_hosts(@namespace, "legacy")
        assert Shared.health(copy) == {:unhealthy, {:plugin_lagging, 2}}
        Table.teardown_all(table, :now)
      end)

    assert [_once] = Regex.scan(~r/injected into its shared copy/, log)
  end

  describe "an http poller" do
    setup do
      %{origin: start_origin(body: "v1", etag: "e1")}
    end

    test "of a relay whose input is durable offers content again until it is taken",
         %{origin: origin} do
      table = bind(poll(origin, "refused_poll"), %{durable_input: true})

      assert {"v1", %{"channel" => "config_in"}} = take_ingest({:error, :refused}, 5_000)
      assert {"v1", _} = take_ingest(:ok, 5_000)
      refute Map.has_key?(Table.workers(table, "refused_poll"), :shared)

      Table.teardown_all(table, :now)
    end

    test "shared casts to each relay and seeds a newly bound one", %{origin: origin} do
      plugin = poll(origin, "two_poll")
      table = bind(plugin, %{})
      assert {"v1", %{"channel" => "config_in"}} = cast_frame()

      other = other_relay(plugin, %{}, :ok)
      assert_receive {:other, :cast, "v1"}, 5_000
      refute_received {:"$gen_cast", _}

      unbind(other)
      # The fake origin answers any conditional request 304 while it has a tag.
      set(origin, body: "v2", etag: nil)
      assert {"v2", _} = cast_frame()
      Table.teardown_all(table, :now)
    end

    test "shared, a relay refusing what it is handed holds back no other",
         %{origin: origin} do
      plugin = poll(origin, "refusing_poll")
      table = bind(plugin, %{})
      refusing = other_relay(plugin, %{}, {:error, :refused})
      assert {"v1", _} = cast_frame()

      # The validator was committed, so the next poll asks for a change only.
      assert NodeTestHelper.wait_until(fn -> conditional_requests(origin) >= 1 end, 5_000)

      unbind(refusing)
      Table.teardown_all(table, :now)
    end
  end
end
