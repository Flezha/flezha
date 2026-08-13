defmodule Flezha.Plugin.Provider.WebsocketIngestModeTest do
  @moduledoc """
  §7.2 source park, adapter side: under the effectively-once tier
  (`sync_ingest?` from the activation env) the websocket ingests via a
  blocking call — a zero-credit park then stops the connection process
  reading its socket. Tier-off keeps the fire-and-forget cast unchanged.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Websocket

  defmodule StubOverwatch do
    use GenServer

    def start_link(test_pid), do: GenServer.start_link(__MODULE__, test_pid)

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:io_in, message, metadata}, _from, test_pid) do
      send(test_pid, {:sync_io_in, message, metadata})
      {:reply, :ok, test_pid}
    end

    @impl true
    def handle_cast({:io_in, message, metadata}, test_pid) do
      send(test_pid, {:cast_io_in, message, metadata})
      {:noreply, test_pid}
    end
  end

  defp conn_state(overwatcher, overrides) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "ingest_mode_ws",
        post_connect: %{},
        overwatcher_pid: overwatcher,
        payload_metadata: %{"input_type" => "2"},
        idle_timeout_ms: 0,
        resubscribe_after_ms: 0,
        ignore_pattern: nil,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now
      },
      Map.new(overrides)
    )
  end

  test "tier ingest awaits the journal ack (blocking call)" do
    {:ok, stub} = StubOverwatch.start_link(self())
    state = conn_state(stub, sync_ingest?: true)

    assert {:ok, _state} = Websocket.handle_in({:text, "frame-1"}, state)
    assert_receive {:sync_io_in, "frame-1", %{"input_type" => "2"}}
    refute_receive {:cast_io_in, _, _}
  end

  test "tier-off ingest keeps the fire-and-forget cast" do
    {:ok, stub} = StubOverwatch.start_link(self())

    for state <- [conn_state(stub, sync_ingest?: false), conn_state(stub, [])] do
      assert {:ok, _state} = Websocket.handle_in({:text, "frame-2"}, state)
      assert_receive {:cast_io_in, "frame-2", _}
      refute_receive {:sync_io_in, _, _}
    end
  end
end
