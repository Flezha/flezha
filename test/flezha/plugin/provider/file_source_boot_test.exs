defmodule Flezha.Plugin.Provider.FileSourceBootTest do
  @moduledoc """
  A file source binding stored with `on_terminate` comes up when its relay
  boots: a source declares no `on_terminate/2` of its own, so the kit's is what
  lets the hook subscribe, and the reader then feeds the live relay.
  """

  # async: false: writes Application env; the live controller and its fleet.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.File.Source
  alias Flezha.Relay.Builder
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager

  @namespace "default"

  test "a stored binding naming on_terminate binds at boot and feeds", %{tmp: tmp} do
    {name, relay_id} = {"file_source_boot_plugin", "file_source_boot_op"}
    Application.put_env(:flezha, :file_plugin_root, tmp)
    File.write!(Path.join(tmp, "events.ndjson"), "a\nb\n")
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    on_exit(fn -> PluginManager.disable_plugin(@namespace, name) end)

    plugin =
      Plugin.new(
        id: name,
        provider: Source,
        opts: %{path: "events.ndjson", channel_in: "events"},
        hooks: [{:on_terminate, []}],
        namespace: @namespace
      )

    assert {:ok, _} = PluginManager.enable_plugin(name, plugin)

    # Started from its declared bindings, as a restart rebuilds it.
    assert {:ok, _} =
             RelayManager.start_relay(@namespace, %Builder{
               id: relay_id,
               factory_id: "store_input",
               opts: [bindings: [{name, %{plugin | opts: %{}}}]]
             })

    table = fn ->
      {:ok, %{extensions: table}} = RelayManager.get_overwatch_state(@namespace, relay_id)
      table
    end

    assert wait_until(fn -> Table.running?(table.(), name) end, 5_000)
    reader = Table.owned_pid(table.(), name)
    # The offset moves only past a line the relay confirmed.
    assert wait_until(fn -> :sys.get_state(reader).offset == 4 end, 5_000)
  end
end
