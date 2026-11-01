defmodule Flezha.Plugin.Provider.FileSourceRestartTest do
  @moduledoc """
  A file source's committed offset survives a controller restart, a move of its
  file into the namespace's folder and a disable and enable of the plugin: the
  commit is written to the declared manifest a restart rebuilds the relay from,
  and a reader prepared from that manifest starts at it.

  The relay is `HubRegistryTestHelper.StubChild`, so the restart is real
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

    Application.put_env(:flezha, :file_plugin_root, Path.join(tmp, "files"))
    # The namespace's own folder, where its plugin reads.
    files = Path.join([tmp, "files", @namespace])
    File.mkdir_p!(files)

    %{files: files}
  end

  test "a reader rebuilt from the declared manifest starts at the committed offset",
       %{files: files} do
    reader = restarted_reader(files)

    # The first poll has opened the file: nothing before the offset is offered.
    assert wait_until(fn -> :sys.get_state(reader).identity != nil end, 2_000)
    refute_ingest()

    File.write!(Path.join(files, "events.ndjson"), "d\n", [:append])
    send(reader, :poll)
    assert {"d", %{"channel" => "events"}} = take_ingest()
  end

  # The upgrade that gave each namespace its own folder: the file is moved from
  # the root into the namespace's folder while the controller is down, and the
  # plugin, enabled with a relative path, is not touched.
  test "a file moved into its namespace's folder keeps its read position", %{files: files} do
    reader = restarted_reader(files, written_at: Path.join(Path.dirname(files), "events.ndjson"))

    assert wait_until(fn -> :sys.get_state(reader).identity != nil end, 2_000)
    refute_ingest()
  end

  # The offset is the relay's binding's, so a disable and an enable of the same
  # plugin, as the upgrade notes allow, does not reset it.
  test "a plugin disabled and enabled again keeps its read position", %{files: files} do
    reader = restarted_reader(files, enable_again: true)

    assert wait_until(fn -> :sys.get_state(reader).identity != nil end, 2_000)
    refute_ingest()
  end

  # Enables the source in its namespace, commits the offset after "a\nb\n" of
  # "a\nb\nc\n", restarts the hub and starts the reader from what the restart
  # rebuilt. `written_at:` is where the file sits before the restart, the
  # shared root of the layout before namespaces had folders; it is moved into
  # the namespace's folder while the hub is down.
  defp restarted_reader(files, opts \\ []) do
    hub_id = Identificator.hub(@namespace)
    relay_id = "file_source_restart_op"
    plugin_id = "file_source_restart_plugin"
    events = Path.join(files, "events.ndjson")
    written_at = Keyword.get(opts, :written_at, events)
    File.write!(written_at, "a\nb\nc\n")

    HubRegistryTestHelper.seed_runner(@namespace, "file_source_runner", "market")
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)

    :ok =
      HubRegistryTestHelper.start_durable(@namespace, relay_id,
        recorded_runner: nil,
        opts: []
      )

    plugin =
      Plugin.new(
        id: plugin_id,
        provider: Source,
        opts: %{path: "events.ndjson", channel_in: "events"},
        hooks: [{:on_terminate, []}],
        namespace: @namespace,
        relay_id: relay_id
      )

    assert {:ok, _} = PluginManager.enable_plugin(plugin_id, plugin)

    # The durable half of what the Overwatch does with a reader's commit: a
    # reader's offset is its relay's position, so it is the binding's.
    assert :ok = PluginManager.persist_opt(@namespace, relay_id, plugin_id, :offset, 6)

    if opts[:enable_again] do
      assert {:ok, _} = PluginManager.disable_plugin(@namespace, plugin_id)
      assert {:ok, _} = PluginManager.enable_plugin(plugin_id, plugin)
    end

    HubRegistryTestHelper.stop_hub_keeping_registry(@namespace)
    if written_at != events, do: File.rename!(written_at, events)
    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 10_000)
    assert HubRegistryTestHelper.await_bound_node(hub_id, relay_id, 10_000) == node()

    assert [{^plugin_id, %Plugin{} = binding}] =
             HubRegistryTestHelper.started_plugins(hub_id, relay_id)

    # What the reader is brought up from: the binding laid over its entry.
    assert {:ok, %Plugin{opts: declared}} = PluginManager.resolve(@namespace, binding)
    assert declared.offset == 6

    {:ok, _prepared, reader} =
      ProviderPrepare.prepare_and_activate(
        Source,
        Map.merge(declared, %{plugin_id: plugin_id, source_namespace: @namespace})
      )

    ProviderPrepare.await_stop_on_exit(reader)
    reader
  end
end
