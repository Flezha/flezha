defmodule Flezha.Service.RelayListenNoHubTest do
  @moduledoc """
  A plugin enabled in a namespace no runner has brought a hub up in (here by
  `relay listen` and by `relay start` with a plugin) is refused
  `namespace.not_running`. It uses the real `Directory`, which
  `listener_test.exs` stubs.
  """
  use ExUnit.Case, async: false

  alias Flezha.Cluster.Directory
  alias Flezha.Service.Admin
  alias Flezha.Service.Cli.Context

  @ns "kb708-no-hub"

  setup do
    assert Directory.hub_nodes(@ns) == []
    :ok
  end

  # Fails when: the hub-less enable behind a listener answers a reason with no
  # catalogue entry at this site (`internal.uncatalogued`).
  test "listening in a namespace with no hub is refused namespace.not_running" do
    args = Jason.encode!([@ns, "r1", "output", %{window_ms: 200}])
    context = Map.put(Context.cli_context([]), :emit, fn _event -> :ok end)

    assert {:error, %Flezha.Error{code: "namespace.not_running", details: details}} =
             Admin.run("Relay.listen", args, context, :given)

    assert details["namespace"] == @ns
  end

  # Fails when: a plugin spec naming a provider reaches the same enable from
  # `relay start` and is answered uncatalogued.
  test "starting a relay with a plugin in a namespace with no hub is refused namespace.not_running" do
    plugins = [%{name: "p1", provider: "relay_stdout"}]
    args = Jason.encode!([@ns, %{id: "r1", factory_id: "f1", plugins: plugins}])

    assert {:error, %Flezha.Error{code: "namespace.not_running"}} =
             Admin.run("Relay.start", args, Context.cli_context([]), :given)
  end
end
