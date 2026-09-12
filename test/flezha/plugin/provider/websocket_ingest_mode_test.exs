defmodule Flezha.Plugin.Provider.WebsocketIngestModeTest do
  @moduledoc """
  §7.2 source park, adapter side: under the effectively-once tier
  (`sync_ingest?` from the activation env) the websocket ingests via a
  blocking call — a zero-credit park then stops the connection process
  reading its socket. Tier-off keeps the fire-and-forget cast unchanged.
  """
  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.Websocket
  alias FlezhaTest.Helper.WebsocketSourceFixtures

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

  # The socket state `start_link/1` builds, from the one fixture every websocket
  # test drives: this file overrides the ingest half and nothing else.
  defp conn_state(overwatcher, overrides) do
    WebsocketSourceFixtures.state(
      Map.merge(
        %{
          plugin_id: "ingest_mode_ws",
          overwatcher_pid: overwatcher,
          payload_metadata: %{"input_type" => "2"},
          subscription: nil
        },
        Map.new(overrides)
      )
    )
  end

  test "tier ingest awaits the journal ack (blocking call)" do
    {:ok, stub} = StubOverwatch.start_link(self())
    state = conn_state(stub, sync_ingest?: true)

    assert {:ok, _state} = Websocket.Client.handle_in({:text, "frame-1"}, state)
    assert_receive {:sync_io_in, "frame-1", %{"input_type" => "2"}}
    refute_received {:cast_io_in, _, _}
  end

  test "tier-off ingest keeps the fire-and-forget cast" do
    {:ok, stub} = StubOverwatch.start_link(self())

    for state <- [conn_state(stub, sync_ingest?: false), conn_state(stub, [])] do
      assert {:ok, _state} = Websocket.Client.handle_in({:text, "frame-2"}, state)
      assert_receive {:cast_io_in, "frame-2", _}
      refute_received {:sync_io_in, _, _}
    end
  end
end
