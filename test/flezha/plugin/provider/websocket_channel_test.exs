defmodule FlezhaTest.Plugin.Provider.WebsocketChannelTest do
  @moduledoc """
  The websocket plugin's INPUT-side channel: the entry channel its inbound
  frames are injected on, named by an option rather than by a key a user
  had to hide inside the general metadata bag.

  Driven at the same seam the other websocket test files use — the provider
  callbacks and `handle_in/2`, never over a live socket — because what decides
  the entry channel is the metadata the worker is handed, and that is settled
  when the plugin is enabled.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.Websocket.Client
  alias FlezhaTest.Helper.ProviderPrepare
  alias FlezhaTest.Helper.WebsocketSourceFixtures

  @uri "wss://flezha-test.invalid:9/never-connect"

  defmodule StubOverwatch do
    @moduledoc false
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_cast({:io_in, message, metadata}, test_pid) do
      send(test_pid, {:injected, message, metadata})
      {:noreply, test_pid}
    end
  end

  defp prepared(extra, subscriptions) do
    ProviderPrepare.prepare(
      Websocket,
      Map.merge(
        %{
          uri: @uri,
          plugin_id: "ws_channel_#{System.unique_integer([:positive])}",
          subscribed_extensions: subscriptions
        },
        extra
      )
    )
  end

  # The metadata one inbound frame is injected with, from enable to injection:
  # the plugin's options through the declared worker spec, then one frame
  # through the socket process's own `handle_in/2`.
  defp injected_metadata(extra, subscriptions \\ []) do
    {:ok, opts} = prepared(extra, subscriptions)
    injected_with(opts)
  end

  defp injected_with(opts) do
    [{_key, worker_opts}] = Websocket.worker_specs(opts)
    {:ok, overwatch} = StubOverwatch.start_link(self())

    targets = for target <- worker_opts.targets, do: %{target | pid: overwatch}

    state =
      WebsocketSourceFixtures.state(%{
        overwatcher_pid: overwatch,
        payload_metadata: worker_opts.payload_metadata,
        targets: Client.ingest_targets(targets, worker_opts.payload_metadata),
        plugin_id: worker_opts.plugin_id,
        sink: worker_opts.sink,
        subscription: nil
      })

    assert {:ok, _state} = Client.handle_in({:text, "frame"}, state)
    assert_receive {:injected, "frame", metadata}, 1_000
    metadata
  end

  describe "the entry channel is an option" do
    test "a receive-only plugin injects on the channel its `channel` names" do
      assert injected_metadata(%{channel: "us"}) == %{"channel" => "us"}
    end

    test "`channel_in` names the same channel" do
      assert injected_metadata(%{channel_in: "us"}) == %{"channel" => "us"}
    end

    test "the other labels of the bag still ride along" do
      assert injected_metadata(%{channel: "us", payload_metadata: %{"input_type" => "2"}}) ==
               %{"channel" => "us", "input_type" => "2"}
    end

    test "the bag alone still selects the entry channel" do
      assert injected_metadata(%{payload_metadata: %{"channel" => "us"}}) ==
               %{"channel" => "us"}
    end

    test "a plugin naming no channel injects no channel, as before" do
      assert injected_metadata(%{}) == %{}
    end

    test "the bag and an option together are refused, naming both" do
      assert {:error, {:channel_written_twice, "eu", "us"}} =
               prepared(%{channel: "us", payload_metadata: %{"channel" => "eu"}}, [])
    end

    # The filter half is a set, and a user reads back what they typed.
    test "a filter of several names is named in the user's own spelling" do
      assert {:error, {:channel_written_twice, "eu", "alerts,audit"}} =
               prepared(
                 %{channel_out: "alerts,audit", payload_metadata: %{"channel" => "eu"}},
                 [:on_output]
               )
    end
  end

  # A plugin whose sink worker is registered where the real one registers: a
  # negative assertion needs it as much as a positive one, since a provider with
  # no worker at all refuses the ask whatever its filter says.
  defp stub_sink(_ctx) do
    plugin_id = "ws_sink_#{System.unique_integer([:positive])}"
    ProviderPrepare.stub_sink_worker(Websocket, plugin_id)
    {:ok, plugin_id: plugin_id}
  end

  describe "one plugin, both directions" do
    setup :stub_sink

    defp ctx(metadata) do
      %Ctx.IO{operator_id: "op", namespace: :default, message: "out", metadata: metadata}
    end

    test "one `channel` names the entry channel and the output filter",
         %{plugin_id: plugin_id} do
      {:ok, opts} = prepared(%{plugin_id: plugin_id, channel: "prices"}, [:on_output])

      assert injected_with(opts) == %{"channel" => "prices"}

      assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => "prices"}), opts)
      assert_receive {:shipped, "out", _}, 1_000

      assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => "alerts"}), opts)
      refute_receive {:shipped, _, _}, 200
    end

    test "the pair names one end each", %{plugin_id: plugin_id} do
      {:ok, opts} =
        prepared(%{plugin_id: plugin_id, channel_in: "requests", channel_out: "responses"}, [
          :on_output
        ])

      assert injected_with(opts) == %{"channel" => "requests"}

      assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => "responses"}), opts)
      assert_receive {:shipped, "out", _}, 1_000

      assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => "requests"}), opts)
      refute_receive {:shipped, _, _}, 200
    end

    test "`channel_out` alone leaves the entry channel unnamed", %{plugin_id: plugin_id} do
      {:ok, opts} = prepared(%{plugin_id: plugin_id, channel_out: "prices"}, [:on_output])

      assert injected_with(opts) == %{}
    end
  end

  describe "a filter naming several channels" do
    setup :stub_sink

    test "each named channel is shipped and no other is", %{plugin_id: plugin_id} do
      {:ok, opts} = prepared(%{plugin_id: plugin_id, channel_out: "alerts,audit"}, [:on_output])

      for channel <- ["alerts", "audit"] do
        assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => channel}), opts)
        assert_receive {:shipped, "out", %{"channel" => ^channel}}, 1_000
      end

      assert {:ok, _} = Websocket.on_output(ctx(%{"channel" => "metrics"}), opts)
      refute_receive {:shipped, _, _}, 200
    end
  end

  # `Table.attempt_restart/3` falls back to the convergence loop's DECLARED
  # manifest for a plugin the table does not hold, and the teardown asks the
  # provider for its declared worker keys from those raw user options: no
  # host key merged, no row default filled. `Workers.declared_keys/3` turns a
  # raise there into an empty key list, so a worker its own supervisor restarted
  # under that key would never be stopped.
  describe "the declared worker keys are readable from raw user options" do
    test "a map that was never normalised still answers a worker spec" do
      raw = %{uri: @uri, plugin_id: "ws_raw", channel: "prices"}

      assert [{"ws_raw", worker_opts}] = Websocket.worker_specs(raw)

      # Nothing resolved the channel, so nothing names one: the metadata is the
      # bag as written, which here is the row default nobody filled in.
      assert worker_opts.payload_metadata == %{}
    end
  end

  describe "a key naming a job the plugin does not do is refused" do
    test "`channel_out` on a plugin with no chain hook" do
      assert {:error, {:channel_unused, "websocket", :channel_out, :out}} =
               prepared(%{channel_out: "prices"}, [])
    end

    test "`channel` together with an explicit key" do
      assert {:error, {:channel_spelled_twice, "websocket", [:channel_out]}} =
               prepared(%{channel: "a", channel_out: "b"}, [:on_output])
    end
  end
end
