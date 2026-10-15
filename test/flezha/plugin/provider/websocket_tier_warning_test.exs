defmodule FlezhaTest.Plugin.Provider.WebsocketTierWarningTest do
  @moduledoc """
  A websocket bound with `on_output` to an effectively-once relay warns that its
  output hop is best effort. The tier reaches a provider only in the activation
  env (`Relay.Plugins.env/1`), so the binding is driven the way a relay drives
  it: subscribe, then activate with the env.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Shared
  alias Flezha.Relay.Overwatch
  alias FlezhaTest.Helper.WebsocketServer

  setup do
    # Test env caps the primary logger level at :error; the tier warning is a
    # :warning, so open this module up for the capture and close it after.
    :ok = :logger.set_module_level(Websocket, :all)
    on_exit(fn -> :logger.unset_module_level(Websocket) end)
    %{server: WebsocketServer.start()}
  end

  # Fails when: the warning is raised only from `init_provider/1`, which runs
  # at subscribe, before the env carrying the tier is merged at activation.
  test "warns for an effectively-once relay", %{server: server} do
    log = bring_up(server, [{:on_output, %{}}], %{effectively_once: true, durable_input: true})

    assert log =~ "effectively-once relay"
    assert log =~ ~s(sink_ack_mode="none")
  end

  # A journaling relay below the tier also activates its own copy, so the
  # warning's activation code runs and must stay silent.
  test "does not warn for a relay below the tier", %{server: server} do
    log = bring_up(server, [{:on_output, %{}}], %{effectively_once: false, durable_input: true})

    assert log =~ "init_provider prepared"
    refute log =~ "effectively-once relay"
  end

  test "does not warn for a plugin bound in neither direction", %{server: server} do
    log = bring_up(server, [], %{effectively_once: true, durable_input: true})

    assert log =~ "sink=false"
    refute log =~ "effectively-once relay"
  end

  defp bring_up(server, hooks, env) do
    plugin = %Plugin{
      id: "ws_out",
      provider: Websocket,
      namespace: "kb683_ns",
      relay_id: "op",
      hooks: hooks,
      opts: %{uri: "ws://127.0.0.1:#{server.port}/feed"}
    }

    capture_log(fn ->
      {:ok, table} =
        Overwatch.__extensions_catalog__()
        |> Table.new(shared: Shared)
        |> Table.bulk_subscribe([{plugin.id, plugin}])

      assert {table, []} = Table.bulk_activate(table, env)
      assert Table.running?(table, plugin.id)
      _ = Table.teardown_all(table)
    end)
  end
end
