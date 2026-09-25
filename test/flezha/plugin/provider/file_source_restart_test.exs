defmodule Flezha.Plugin.Provider.FileSourceRestartTest do
  @moduledoc """
  A file source's committed offset survives a controller restart: the commit is
  written to the declared manifest a restart rebuilds the operator from, and a
  reader prepared from that manifest starts at it.

  The operator is `HubRegistryTestHelper.StubChild`, so the restart is real
  and the plugin list it rebuilds from is read off the stub's start argument.
  """

  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.ProviderPrepare, only: [take_ingest: 0, refute_ingest: 0]

  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.File.Source
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.ProviderPrepare

  @namespace "file_source_restart_ns"

  setup %{tmp: tmp} do
    :ok = HubRegistryTestHelper.recovering_hub(@namespace, Path.join(tmp, "hub"), 50)

    files = Path.join(tmp, "files")
    File.mkdir_p!(files)
    Application.put_env(:flezha, :file_plugin_root, files)

    %{files: files}
  end

  test "a reader rebuilt from the declared manifest starts at the committed offset",
       %{files: files} do
    hub_id = Identificator.hub(@namespace)
    operator_id = "file_source_restart_op"
    plugin_id = "file_source_restart_plugin"

    HubRegistryTestHelper.seed_runner(@namespace, "file_source_runner", "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, operator_id,
        recorded_runner: nil,
        opts: []
      )

    on_exit(fn -> PluginManager.unregister_plugin(plugin_id) end)

    plugin =
      Plugin.new(
        id: plugin_id,
        provider: Source,
        opts: %{path: "events.ndjson", channel_in: "events"},
        hooks: [{:on_terminate, []}],
        namespace: @namespace,
        operator_id: operator_id
      )

    assert {:ok, _} = PluginManager.enable_plugin(plugin_id, plugin)

    # The durable half of what the Overwatch does with a reader's commit.
    assert :ok = PluginManager.persist_plugin_opt(@namespace, operator_id, plugin_id, :offset, 6)

    HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)
    assert HubRegistryTestHelper.await_bound_node(hub_id, operator_id, 10_000) == node()

    assert [{^plugin_id, %Plugin{opts: declared}}] =
             HubRegistryTestHelper.started_plugins(hub_id, operator_id)

    assert declared.offset == 6

    File.write!(Path.join(files, "events.ndjson"), "a\nb\nc\n")

    {:ok, _prepared, reader} =
      ProviderPrepare.prepare_and_activate(Source, Map.put(declared, :plugin_id, plugin_id))

    ProviderPrepare.await_stop_on_exit(reader)

    # The first poll has opened the file: nothing before the offset is offered.
    assert wait_until(fn -> :sys.get_state(reader).identity != nil end, 2_000)
    refute_ingest()

    File.write!(Path.join(files, "events.ndjson"), "d\n", [:append])
    send(reader, :poll)
    assert {"d", %{"channel" => "events"}} = take_ingest()
  end
end
