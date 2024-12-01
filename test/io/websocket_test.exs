defmodule AflechaTest.IO.WebsocketTest do
  use ExUnit.Case
  doctest Aflecha.IO.Websocket

  setup_all do
    %{}
  end

  test "start_link" do
    io_settings = %Aflecha.IO.Websocket{
      uri: "wss://ws.kraken.com",
      post_connect: %{
        payload: {
          :text,
          "{\"event\":\"subscribe\", \"subscription\":{\"name\": \"trade\"}, \"pair\":[\"BTC/EUR\", \"ETH/EUR\"]}"
        }
      }
    }

    {:ok, pid} = Aflecha.IO.Websocket.start_link(io_settings)

    # Aflecha.IO.Websocket.send_payload(
    #   pid,
    #   {:text,
    #    "{\"event\":\"subscribe\", \"subscription\":{\"name\": \"trade\"}, \"pair\":[\"BTC/EUR\", \"ETH/EUR\"]}"}
    # )

    Process.sleep(10000)

    # mfa = Aflecha.IO.Provider.mfa(io_settings)
    # Aflecha.IO.IOSupervisor.start_child(Aflecha.IO.IOSupervisor, mfa)
    # {:ok, pid} = Aflecha.IO.IOSupervisor.find_pid(Aflecha.IO.IOSupervisor, mfa)

    assert true
  end
end
