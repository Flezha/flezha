defmodule FlezhaTest.Plugin.Provider.WebsocketLifecycleTest do
  @moduledoc """
  Asserts `Plugin.Provider.Websocket.init_provider/1` does NOT open a TLS
  connection — that work moves to `activate/1` per
  `fix-overwatch-restart-storm` capability `operator-extensions` "Plugin
  providers SHALL NOT perform upstream I/O in `init_provider/1`".

  Approach: hand `init_provider/1` an unroutable URI and require it
  returns `{:ok, opts}` quickly without raising or blocking. If the TLS
  handshake fires, the call would block on DNS / connect attempts for
  many seconds and the test would either time out or raise; the
  assertion is "return quickly with the prepared opts and no failure".
  """

  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  use ExUnit.Case, async: true

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2]

  # Preparation as the table performs it: the declaration normalises the
  # options, then the provider interprets them.
  defp prepare(opts) do
    with {:ok, opts} <- ProviderOptions.normalise_opts(Websocket, opts),
         do: prepare(Websocket, opts)
  end

  test "init_provider returns prepared opts without opening TLS connection" do
    opts = %{
      uri: "wss://flezha-test.invalid:9/never-connect",
      plugin_id: "ws_lifecycle_test",
      overwatcher_pid: self()
    }

    {time_us, result} = :timer.tc(fn -> prepare(opts) end)

    assert {:ok, prepared_opts} = result

    # < 250 ms is comfortably below any DNS-then-TCP-connect attempt.
    assert time_us < 250_000,
           "init_provider took #{time_us}us — it must NOT block on upstream I/O"

    assert prepared_opts.uri == opts.uri
    assert prepared_opts.plugin_id == opts.plugin_id
    # The host identity the table merges, which the declared worker reads.
    assert is_pid(prepared_opts.overwatcher_pid)
  end

  # A partition group enables one plugin per member, each under the member's
  # own id and all on the same URI. The worker is registered by `plugin_id`,
  # so a default taken from the URI put every member on one socket, filtered
  # to the first member's key groups.
  test "with no plugin_id, the worker is named by the id the plugin was enabled under" do
    members =
      for id <- ["exchange#p0", "exchange#p1"] do
        {:ok, prepared} =
          prepare(Websocket, %{
            uri: "wss://flezha-test.invalid:9/never-connect",
            source_plugin_id: id
          })

        prepared.plugin_id
      end

    assert members == ["exchange#p0", "exchange#p1"]
  end

  describe "post_connect is validated at preparation" do
    defp prepared(post_connect) do
      prepare(%{
        uri: "wss://flezha-test.invalid:9/never-connect",
        plugin_id: "ws_post_connect_test",
        post_connect: post_connect
      })
    end

    test "a frame type that is neither text nor binary is refused" do
      assert {:error, message} =
               prepared(%{"payload" => %{"type" => "Binary", "message" => "hi"}})

      assert message =~ "type"
      assert message =~ "Binary"
    end

    test "a payload that cannot be decoded is refused, not silently emptied" do
      assert {:error, message} = prepared("{")
      assert is_binary(message)
    end

    test "both frame types are accepted, as a map and as its JSON string" do
      for type <- ["text", "binary"] do
        assert {:ok, opts} = prepared(%{"payload" => %{"type" => type, "message" => "hi"}})
        assert opts.post_connect["payload"]["type"] == type

        assert {:ok, opts} = prepared(~s({"payload":{"type":"#{type}","message":"hi"}}))
        assert opts.post_connect["payload"]["type"] == type
      end
    end

    test "no post_connect at all is still the empty map" do
      assert {:ok, opts} = prepared(nil)
      assert opts.post_connect == %{}
    end
  end
end
