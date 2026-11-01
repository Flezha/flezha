defmodule Flezha.Plugin.Provider.FileSourceBootTest do
  @moduledoc """
  A file source binding stored with `on_terminate` comes up when its relay
  boots: a source declares no `on_terminate/2` of its own, so the kit's is what
  lets the hook subscribe, and the reader then feeds the live relay. A binding
  stored with a path outside its namespace's folder does not come up.
  """

  # async: false: writes Application env; the live controller and its fleet.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, with_only_logged: 2]

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.File.Sink
  alias Flezha.Plugin.Provider.File.Source
  alias Flezha.Relay.Builder
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager

  @namespace "default"

  test "a stored binding naming on_terminate binds at boot and feeds", %{tmp: tmp} do
    {name, relay_id} = {"file_source_boot_plugin", "file_source_boot_op"}
    Application.put_env(:flezha, :file_plugin_root, tmp)
    File.mkdir_p!(Path.join(tmp, @namespace))
    File.write!(Path.join([tmp, @namespace, "events.ndjson"]), "a\nb\n")
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    on_exit(fn -> PluginManager.disable_plugin(@namespace, name) end)

    plugin =
      Plugin.new(
        id: name,
        provider: Source,
        # The factory's own input channel: a line nothing reads would be
        # dead-lettered into the shared store, not fed to the relay.
        opts: %{path: "events.ndjson", channel_in: "input"},
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

  test "a binding stored under the shared folder is refused on restore, naming the new place",
       %{tmp: tmp} do
    {name, relay_id} = {"file_sink_restore_plugin", "file_sink_restore_op"}
    Application.put_env(:flezha, :file_plugin_root, tmp)
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    on_exit(fn -> PluginManager.disable_plugin(@namespace, name) end)

    # An enable stores a relay plugin's options as written, so this entry is the
    # one a controller wrote before each namespace had its own folder.
    stored = Path.join(tmp, "out.ndjson")

    plugin =
      Plugin.new(
        id: name,
        provider: Sink,
        opts: %{path: stored},
        hooks: [{:on_output, []}],
        namespace: @namespace
      )

    assert {:ok, _} = PluginManager.enable_plugin(name, plugin)

    namespace_folder = Path.join(tmp, @namespace)
    moved = Path.join(namespace_folder, "out.ndjson")

    start = fn ->
      RelayManager.start_relay(@namespace, %Builder{
        id: relay_id,
        factory_id: "store_input",
        opts: [bindings: [{name, %{plugin | opts: %{}}}]]
      })
    end

    assert {:error,
            {:start_failed, ^relay_id,
             {:plugin_prepare_failed, ^name,
              {:file_path_outside_namespace, ^stored, ^namespace_folder, ^moved}}}} =
             with_only_logged([{~r/prepare failed for plugin_id=#{name}/, :always}], start)

    refute File.exists?(stored)
    refute File.exists?(moved)
  end
end
