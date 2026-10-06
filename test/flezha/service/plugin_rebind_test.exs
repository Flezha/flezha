defmodule Flezha.Service.PluginRebindTest do
  @moduledoc """
  `PluginManager.rebind_all/2` binds back what a rescale's quiesce took off a
  group: each member's bindings as they were read before the unbind, whose
  ids carry the member suffix (`src#p1`) while the plugin entry is enabled
  under its base name (`src`).
  """

  # async: false: Mimic global mode.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin
  alias Flezha.Relay.Overwatch
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Init

  setup :set_mimic_global_until_exit

  @namespace "rebind"

  setup do
    parent = self()
    entry = %Plugin{id: "src", provider: "relay_websocket", namespace: @namespace}

    stub(HubManager, :plugin_entry, fn @namespace, name ->
      if name == "src", do: {:ok, entry}, else: :error
    end)

    stub(HubManager, :relay_bindings, fn @namespace, _member -> {:ok, []} end)

    stub(Init, :write_plugins, fn @namespace, member, _rewrite ->
      send(parent, {:written, member}) && :ok
    end)

    stub(RelayManager, :get_relay_pid, fn @namespace, _member -> {:ok, parent} end)

    stub(Overwatch, :plugin_subscribe, fn _pid, id, _binding, ^entry ->
      send(parent, {:subscribed, id}) && :ok
    end)

    :ok
  end

  # Fails when: a rescale that cannot complete its quiesce leaves the group
  # with no sources and no outputs, because the member-suffixed binding ids
  # are looked up as plugin names and none is found.
  test "every member's suffixed binding is bound back to the plugin enabled under its base name" do
    saved =
      for index <- 0..1 do
        member = "demo#p#{index}"
        id = "src#p#{index}"
        {member, [{id, %Plugin{id: id, provider: nil, namespace: @namespace, relay_id: member}}]}
      end

    assert :ok = PluginManager.rebind_all(@namespace, saved)
    assert Enum.sort([recv(:subscribed), recv(:subscribed)]) == ["src#p0", "src#p1"]
    assert Enum.sort([recv(:written), recv(:written)]) == ["demo#p0", "demo#p1"]
  end

  test "a binding whose plugin was disabled meanwhile is named, not silently dropped" do
    saved = [
      {"demo#p0", [{"gone#p0", %Plugin{id: "gone#p0", provider: nil, namespace: @namespace}}]}
    ]

    assert {:error, {:unrebound, ["demo#p0"]}} = PluginManager.rebind_all(@namespace, saved)
  end

  defp recv(tag) do
    receive do
      {^tag, value} -> value
    after
      1_000 -> flunk("no #{tag} message")
    end
  end
end
