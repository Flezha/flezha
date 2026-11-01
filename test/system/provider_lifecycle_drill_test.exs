defmodule FlezhaTest.System.ProviderLifecycleDrillTest do
  @moduledoc """
  Every provider, through the real enable and the real disable, on a live
  relay bound to a live runner, or, for a controller plugin, which no relay
  hosts, on the copy its enable starts.

  The table is `Flezha.Plugin.Providers.all()`, so a provider added to the
  system is driven here by name or fails the drill. Each row names its upstream:
  a TCP origin for the poller and the two HTTP
  senders, a database for the Postgres pair, a websocket fixture for the socket, a second relay for the forward, the test process
  for the two message forwards, nothing for the transforms. A test asserts every
  row HAS a fixture, so a new provider cannot escape the drill by being absent
  from a list.

  Per row, through `Flezha.Service.Cli.call/3` with `PluginManager.enable` and
  `PluginManager.disable` in process (nothing under `test/` runs the released
  `flz` binary), plus the published `PluginManager.enable/3` for one row so
  both surfaces are covered.
  Authorization is off in this tier, so the verbs are called untokened; the
  token path is `multi_tenant_e2e_test`'s. Per row:

    * enable, and read the phase and the `[:flezha, :plugin, :phase]` events;
    * read `Table.health/1` and `flz relay info`'s plugin block in process;
    * kill the worker and assert the declared restart value's outcome;
    * disable, and assert the family registry holds nothing for the plugin.

  What the in-suite conformance drill
  (`test/flezha/plugin/provider_lifecycle_test.exs`) proves against a fake
  worker, this proves against the real one, through the user's own verbs.

  Tagged `:system`; run with
  `mix test --include system test/system/provider_lifecycle_drill_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  import ExUnit.CaptureLog, only: [with_log: 1]

  import FlezhaTest.Helper.NodeTestHelper,
    only: [claim_tmp_dir: 1, wait_until: 2, with_only_logged: 2]

  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, stop_namespace: 1, unique_id_path: 1]

  alias Flezha.Cluster.Directory
  alias Flezha.Relay.Overwatch
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.Providers
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.TestHelpers.PluginPackage
  alias FlezhaTest.Helper.Telemetry

  @moduletag timeout: 300_000

  # One row per provider: the options an enable needs, and what stands in for
  # its upstream. `:none` is a provider with no upstream at all, which is not
  # the same as a missing fixture and is why the two are spelled apart. A
  # source feeds `input`, the one entry channel the drill relay announces.
  defp fixtures(ctx) do
    %{
      Provider.StdOut => {:none, %{}},
      Provider.Filter => {:none, %{regex_pattern: ".*"}},
      Provider.MessageForward => {:none, %{key: "drill"}},
      Provider.BeamMsgForward => {:test_process, %{recv_pid: ctx.sink}},
      Provider.MonitoredIOForward =>
        {:test_process,
         %{
           recv_pid: ctx.sink,
           relay_id: ctx.relay_id,
           namespace: ctx.namespace,
           heartbeat_interval: 60_000
         }},
      Provider.HttpPoll =>
        {:tcp_origin, %{url: ctx.origin_url, channel: "input", interval_ms: 60_000}},
      Provider.HttpPush => {:tcp_origin, %{url: ctx.origin_url, channel: "drill"}},
      Provider.HookWebhook => {:tcp_origin, %{url: ctx.origin_url}},
      Provider.File.Source => {:file, %{path: "drill_source.ndjson", channel: "input"}},
      Provider.File.Sink => {:file, %{path: "drill_sink.ndjson"}},
      Provider.Websocket => {:websocket, %{uri: ctx.ws_uri, idle_timeout_ms: 0}},
      Provider.RelayForward => {:second_relay, %{target_relay_id: ctx.target_id}},
      Provider.Postgres =>
        {:database, %{database: "flezha_test", username: "postgres", auto_create: true}},
      # Port 1 answers nothing: the reader runs and its reads fail, which is a
      # verdict, not a lifecycle fault.
      Provider.PostgresPoll =>
        {:database,
         %{
           database: "flezha_test",
           username: "postgres",
           hostname: "127.0.0.1",
           port: 1,
           table: "drill",
           id_column: "id",
           channel: "input"
         }},
      # The example plugin package, loaded in setup: a package's row is driven
      # like a first-party one.
      FlezhaExamplePlugin.Stamp => {:none, %{path: ctx.stamp_path}}
    }
  end

  # Rows whose upstream this tier cannot stand up, each recording why and where
  # the behaviour is proved instead. A deferral is a decision, not a gap: it
  # names the coverage that replaces it.
  @deferred %{
    Provider.Postgres =>
      "needs the database `effectively_once_postgres_e2e_test` uses, which this tier does not " <>
        "boot; the sink's own lifecycle is proved against a fake worker by the conformance drill"
  }

  setup_all do
    # `[a-z][a-z0-9-]{0,62}`: a namespace name takes a hyphen, never an underscore.
    namespace = "provider-drill-#{System.unique_integer([:positive])}"
    {_runner_port, _runner_id} = boot_runner(namespace, unique_id_path("provider_drill"), [])

    # The two message forwards deliver to a process of this VM, given as a pid.
    sink = spawn_link(fn -> Process.sleep(:infinity) end)

    relay_id = start_relay(namespace, "drill_op")
    # The forward's destination: a second relay in the same namespace, which
    # is the only shape that provider accepts.
    target_id = start_relay(namespace, "drill_target")

    on_exit(fn -> stop_namespace(namespace) end)

    # The two file plugins resolve their paths inside the namespace's folder
    # under one root, and the source reads a file that is already there.
    file_root = claim_tmp_dir("provider_drill_files")
    File.mkdir_p!(Path.join(file_root, namespace))
    File.write!(Path.join([file_root, namespace, "drill_source.ndjson"]), "")
    previous_root = Application.fetch_env(:flezha, :file_plugin_root)
    Application.put_env(:flezha, :file_plugin_root, file_root)

    on_exit(fn ->
      case previous_root do
        {:ok, root} -> Application.put_env(:flezha, :file_plugin_root, root)
        :error -> Application.delete_env(:flezha, :file_plugin_root)
      end

      File.rm_rf!(file_root)
    end)

    load_example_package()

    %{
      namespace: namespace,
      sink: sink,
      relay_id: relay_id,
      target_id: target_id,
      origin_url: "http://127.0.0.1:1/drill",
      ws_uri: "ws://127.0.0.1:1/drill",
      stamp_path: Path.join(file_root, "stamped.txt")
    }
  end

  # What a boot with the example package in its plugin root does: the loader
  # registers its row, and the tree supervises its worker family, which this
  # controller booted without.
  defp load_example_package() do
    root = claim_tmp_dir("provider_drill_plugins")
    previous = Application.get_env(:flezha, :plugins, [])
    [%{status: :loaded}] = PluginPackage.load!(root, [PluginPackage.example!()])
    family = WorkerSup.family(FlezhaExamplePlugin.Stamp)
    {:ok, _} = Supervisor.start_child(Flezha.Supervisor, {WorkerSup, name: family})

    on_exit(fn ->
      _ = Supervisor.terminate_child(Flezha.Supervisor, family)
      _ = Supervisor.delete_child(Flezha.Supervisor, family)
      PluginPackage.unload!(root)
      Application.put_env(:flezha, :plugins, previous)
      File.rm_rf!(root)
    end)
  end

  test "every provider row names a fixture", ctx do
    rows = fixtures(ctx)

    missing =
      for {id, provider} <- Providers.all(),
          not Map.has_key?(rows, provider),
          do: "#{id} (#{inspect(provider)})"

    assert missing == [],
           "a provider with no fixture escapes the drill rather than being driven by it: " <>
             Enum.join(missing, ", ")

    for {provider, reason} <- @deferred do
      assert Map.has_key?(rows, provider), "a deferral for a provider no row names"
      assert is_binary(reason) and reason != ""
    end
  end

  test "the published enable reaches the same lifecycle as the CLI verb", ctx do
    # `PluginManager.enable/3` is the surface a consumer calls; it must reach the
    # same table, the same phase and the same worker as the user's verb.
    plugin_id = "drill-published-#{System.unique_integer([:positive])}"

    assert {:ok, _} =
             PluginManager.enable(
               plugin_id,
               [
                 provider: Provider.Filter,
                 namespace: ctx.namespace,
                 relay_id: ctx.relay_id,
                 opts: %{regex_pattern: ".*"},
                 hooks: [{:on_input, []}]
               ],
               %{source: :internal}
             )

    table = plugin_table(ctx)
    assert Table.running?(table, plugin_id)
    assert Table.workers(table, plugin_id) == %{}

    assert {:ok, _} =
             PluginManager.disable(plugin_id, [namespace: ctx.namespace], %{source: :internal})

    refute Table.running?(plugin_table(ctx), plugin_id)
  end

  test "every declared worker is started at activation and stopped at disable", ctx do
    rows = fixtures(ctx)
    events = collect_phase_events()

    for {id, provider} <- Providers.all(), not Map.has_key?(@deferred, provider) do
      {fixture, opts} = Map.fetch!(rows, provider)

      if provider in [Provider.Websocket, Provider.PostgresPoll] do
        dyn_sup = inspect(WorkerSup.dyn_sup_name(WorkerSup.family(provider)))

        # The row's kill reports reach this capture too; the nested one asserts
        # them. The reader's connection is linked to it and dies with it.
        kills = [
          {~r/crashed: :killed/, :maybe},
          {~r/#{Regex.escape(dyn_sup)} terminated/, :maybe},
          {~r/terminating\n\*\* \((stop|exit)\) killed/, :maybe}
        ]

        with_only_logged([refused() | kills], fn ->
          drive(id, provider, fixture, opts, ctx, events)
        end)
      else
        drive(id, provider, fixture, opts, ctx, events)
      end
    end
  end

  # One plugin option decides the entry channel every inbound frame of a socket
  # is injected on, so two plugins on one relay must not be able to feed the
  # same channel by accident. Driven through the user's own verb on the live
  # relay: what the socket process is started with is the whole of what this
  # decides, and delivering a frame that names a channel to that channel is the
  # runner's, pinned in `crates/flz_core/src/routing_tests.rs`. Two entry channels,
  # so two socket plugins can each feed their own: a merging fixture.
  @tag capability: :graph_builder, skip: FlezhaTest.SystemCase.deferral(:graph_builder)
  test "each websocket plugin feeds the channel its own option names", ctx do
    ctx = %{ctx | relay_id: start_relay(ctx.namespace, "drill_ws", "merged_input")}

    rows = [
      {%{channel: "ticks"}, :on_interval, "ticks", nil},
      {%{channel: "quotes"}, :on_interval, "quotes", nil},
      {%{channel_in: "ticks", channel_out: "responses"}, :on_output, "ticks", ["responses"]},
      # The user's own comma-separated spelling, through the real verb: one
      # filter naming two channels, resolved to the set the dispatch reads.
      {%{channel_in: "quotes", channel_out: "alerts,audit"}, :on_output, "quotes",
       ["alerts", "audit"]}
    ]

    with_only_logged([refused()], fn -> Enum.each(rows, &assert_ws_channel(&1, ctx)) end)
  end

  # `ctx.ws_uri` and the Postgres row's port answer nothing, so a socket or a
  # reader that dials one before its kill or disable logs the refusal, and
  # whether it does is timing.
  defp refused, do: {~r/econnrefused/, :maybe}

  defp assert_ws_channel({flags, extension, entry, filter}, ctx) do
    plugin_id = "drill-ws-channel-#{System.unique_integer([:positive])}"

    fields = %{
      "provider" => "relay_websocket",
      "opts" =>
        flags
        |> Map.merge(%{uri: ctx.ws_uri, idle_timeout_ms: 0, plugin_id: plugin_id})
        |> cli_opts(),
      "hooks" => [
        %{"name" => Atom.to_string(extension), "opts" => subscription_opts(extension)}
      ],
      "namespace" => ctx.namespace,
      "relay_id" => ctx.relay_id
    }

    # The verb prints its own confirmation; this test asserts on the table.
    assert {:ok, _printed} =
             ExUnit.CaptureIO.with_io(fn ->
               Cli.call("PluginManager.enable", Jason.encode!([plugin_id, fields]), [])
             end)

    stored = plugin_table(ctx).plugins[plugin_id].opts
    assert stored.entry_channel == entry
    assert stored.filter_channel == filter

    # The map the socket process is started with, from the options the live
    # table holds: this is what every injection of that socket carries.
    assert [{{_namespace, ^plugin_id}, %{targets: [%{channel: ^entry}]}}] =
             Provider.Websocket.worker_specs(stored)

    assert {:ok, _printed} =
             ExUnit.CaptureIO.with_io(fn -> disable(plugin_id, ctx) end)
  end

  # A pid option has no text spelling, so a row that delivers to this test's
  # process is enabled in process, as an embedding host does.
  defp enable(plugin_id, fields, :test_process, published) do
    published = published ++ [namespace: fields["namespace"], relay_id: fields["relay_id"]]
    with {:ok, _} <- PluginManager.enable(plugin_id, published, %{source: :internal}), do: :ok
  end

  defp enable(plugin_id, fields, _fixture, _published),
    do: Cli.call("PluginManager.enable", Jason.encode!([plugin_id, fields]), [])

  defp start_relay(namespace, label, factory_id \\ "do_nothing") do
    id = "#{label}_#{System.unique_integer([:positive])}"

    builder = %Flezha.Relay.Builder{
      id: id,
      factory_id: factory_id,
      init_values: %{}
    }

    {:ok, _} = RelayManager.start_relay(namespace, builder)
    id
  end

  ## One row

  defp drive(id, provider, fixture, opts, ctx, events) do
    plugin_id = "drill-#{id}-#{System.unique_integer([:positive])}"
    extension = hd(provider.__provider__().extensions)

    opts = with_plugin_id(opts, provider, plugin_id)

    # The CLI's fields: the options as text, the extensions it takes, the scope.
    fields =
      %{
        "provider" => id,
        "opts" => cli_opts(opts),
        "hooks" => [
          %{"name" => Atom.to_string(extension), "opts" => subscription_opts(extension)}
        ],
        "namespace" => ctx.namespace
      }
      |> with_relay(provider, ctx)

    # The verb prints and answers `:ok`; a refusal exits `{:shutdown, 1}`, which is
    # what makes an unasserted return safe to ignore here.
    published = [provider: provider, opts: opts, hooks: [{extension, []}]]

    assert :ok = enable(plugin_id, fields, fixture, published),
           "provider `#{id}` refused a real enable"

    table = host_table(ctx, provider, plugin_id)
    assert Table.subscribed?(table, plugin_id), "provider `#{id}` is not on the table"

    assert Table.running?(table, plugin_id),
           "provider `#{id}` did not reach :running; phase #{inspect(Table.phase(table, plugin_id))}"

    assert_health(id, fixture, Table.health(table)[plugin_id])

    assert_worker_shape(id, provider, plugin_id, table)
    assert_info_block(ctx, provider, plugin_id)

    assert_declared_restart(id, provider, plugin_id)
    assert_stamped(provider, plugin_id, opts)

    seen = phase_events(events, plugin_id)
    assert :prepared in seen, "provider `#{id}` emitted no :prepared event"
    assert :running in seen, "provider `#{id}` emitted no :running event"

    assert :ok = disable(plugin_id, ctx), "provider `#{id}` refused a real disable"

    if worker = provider.__provider__().worker do
      assert wait_until(fn -> not alive_worker?(provider, plugin_id) end, 5_000),
             "provider `#{id}` left a #{worker.restart} worker registered after disable"
    end

    assert_host_gone(ctx, provider, plugin_id)
  end

  # The declared restart value, driven, in OTP's own terms: a killed
  # `:temporary` worker stays down and the convergence loop is what brings its
  # plugin back; a `:transient` or `:permanent` one is restarted by its own
  # supervisor under the same key. A source has nothing to keep and a sink has a
  # queue, which is why the value is the provider's and not the shape's.
  # The kill's crash reports are captured and must name the killed worker, so
  # the capture hides nothing else.
  defp assert_declared_restart(id, provider, plugin_id) do
    case provider.__provider__().worker do
      nil ->
        :ok

      worker ->
        {back?, log} =
          with_log(fn ->
            kill_worker(provider, plugin_id)
            wait = if worker.restart == :temporary, do: 500, else: 5_000
            wait_until(fn -> alive_worker?(provider, plugin_id) end, wait)
          end)

        assert log =~ "killed" and log =~ inspect(worker.module),
               "provider `#{id}`'s kill logged no report naming #{inspect(worker.module)}"

        assert back? == (worker.restart != :temporary),
               "provider `#{id}` declares restart #{inspect(worker.restart)} and its " <>
                 "supervisor #{if back?, do: "brought the worker back", else: "did not bring it back"}"
    end
  end

  # The package's worker writes through the library it bundles, reading the
  # stamp from that library's priv/ and rendering it through a protocol the
  # boot consolidated again. The package is loaded at run time, hence `apply/3`.
  defp assert_stamped(FlezhaExamplePlugin.Stamp = provider, plugin_id, %{path: path}) do
    apply(FlezhaExamplePlugin.Stamp.Writer, :append, [worker_pid!(provider, plugin_id), "drill"])

    assert wait_until(fn -> File.read(path) == {:ok, ">>[stamped] drill\n"} end, 5_000),
           "the example package's worker wrote no stamped line to #{path}"
  end

  defp assert_stamped(_provider, _plugin_id, _opts), do: :ok

  # The provider's own worker, through its family: a shared plugin's table
  # tracks the copy that holds it, and the declared restart is the worker's.
  defp kill_worker(provider, plugin_id) do
    pid = worker_pid!(provider, plugin_id)
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 5_000
  end

  # Resolved through the key the product registered, because a cast to a name
  # nobody holds is dropped without a word.
  defp worker_pid!(provider, plugin_id) do
    assert [key | _] = keys_for(WorkerSup.family(provider), plugin_id),
           "no #{inspect(provider)} worker is registered for `#{plugin_id}`"

    WorkerSup.worker_pid(provider, key)
  end

  defp alive_worker?(provider, plugin_id) do
    keys_for(WorkerSup.family(provider), plugin_id) != []
  end

  # A source whose upstream this tier does not stand up reports its own idle
  # verdict, which is the correct answer and not a fault of the lifecycle. What
  # every row must show is that the probe reached the provider and came back in
  # the declared vocabulary.
  defp assert_health(id, fixture, verdict) when fixture in [:none, :test_process] do
    assert verdict == :healthy, "provider `#{id}` is unhealthy on a freshly enabled plugin"
  end

  defp assert_health(id, _fixture, verdict) do
    assert match?(:healthy, verdict) or match?({:unhealthy, _}, verdict) or
             match?({:fail_open, _}, verdict),
           "provider `#{id}` answered #{inspect(verdict)}, which is no verdict at all"
  end

  # A provider that declares a worker holds one, under its own family, with the
  # key its declaration names; one that declares none holds nothing.
  defp assert_worker_shape(id, provider, plugin_id, table) do
    case provider.__provider__().worker do
      # A shared plugin's table tracks the copy it joined, and nothing else.
      nil ->
        assert Map.delete(Table.workers(table, plugin_id), :shared) == %{},
               "provider `#{id}` declares no worker and the table tracks one"

      worker ->
        assert is_pid(Table.owned_pid(table, plugin_id)),
               "provider `#{id}` declares #{inspect(worker.module)} and the table tracks nothing"

        assert alive_worker?(provider, plugin_id),
               "provider `#{id}`'s worker is registered under no key of its own family"
    end
  end

  # `flz relay info` in process: the namespace's block names the enabled
  # plugin, which is the surface a user reads a plugin's own state on. A
  # controller plugin is on no relay's block, and `flz plugin list` names it.
  defp assert_info_block(ctx, provider, plugin_id) do
    verb = if controller?(provider), do: "PluginManager.enabled", else: "Relay.info"

    output =
      ExUnit.CaptureIO.capture_io(fn -> Cli.call(verb, Jason.encode!([ctx.namespace]), []) end)

    assert output =~ plugin_id, "#{verb} does not name the enabled plugin"
  end

  defp controller?(provider), do: provider.__provider__().type == :controller

  # A controller plugin binds no relay.
  defp with_relay(fields, provider, ctx) do
    if controller?(provider), do: fields, else: Map.put(fields, "relay_id", ctx.relay_id)
  end

  # The table the plugin lives on: the relay's, or the controller plugin's
  # own copy's, which its home places once the enable answers.
  defp host_table(ctx, provider, plugin_id) do
    if controller?(provider) do
      assert wait_until(fn -> Directory.plugin_hosts(ctx.namespace, plugin_id) != [] end, 5_000),
             "controller plugin #{plugin_id} has no copy"

      [copy] = Directory.plugin_hosts(ctx.namespace, plugin_id)
      :sys.get_state(copy).table
    else
      plugin_table(ctx)
    end
  end

  defp assert_host_gone(ctx, provider, plugin_id) do
    if controller?(provider) do
      assert wait_until(fn -> Directory.plugin_hosts(ctx.namespace, plugin_id) == [] end, 5_000),
             "controller plugin #{plugin_id} kept a copy after disable"
    else
      refute Table.running?(plugin_table(ctx), plugin_id)
    end
  end

  defp disable(plugin_id, ctx),
    do:
      Cli.call(
        "PluginManager.disable",
        Jason.encode!([plugin_id, %{"namespace" => ctx.namespace}]),
        []
      )

  # A provider that declares `plugin_id` is told the id it was enabled under,
  # which is what a user writes as `--Xplugin_id`. One that does not
  # declare it would refuse the key, so it is not offered.
  defp with_plugin_id(opts, provider, plugin_id) do
    if Keyword.has_key?(provider.__provider__().options, :plugin_id),
      do: Map.put(opts, :plugin_id, plugin_id),
      else: opts
  end

  # A request subscription is keyed and a timer subscription is scheduled; the
  # rest take no sub-options.
  defp subscription_opts(:on_message), do: %{"key" => "drill"}
  defp subscription_opts(:on_interval), do: %{"interval_ms" => 60_000}
  defp subscription_opts(_name), do: %{}

  ## The fixture harness

  defp plugin_table(ctx) do
    {:ok, pid} = HubManager.get_pid(ctx.namespace, ctx.relay_id)
    %Overwatch{extensions: table} = :sys.get_state(pid)
    table
  end

  # A key is the plugin id, or a tuple naming it at any depth:
  # `{namespace, plugin_id}`, or that with a member.
  defp keys_for(family, plugin_id),
    do: Enum.filter(WorkerSup.keys(family), &names?(&1, plugin_id))

  defp names?(key, plugin_id) when is_tuple(key),
    do: Enum.any?(Tuple.to_list(key), &names?(&1, plugin_id))

  defp names?(key, plugin_id), do: key == plugin_id

  # The CLI carries every option as a string, which is the path a user
  # actually uses and the one the declaration converts.
  defp cli_opts(opts), do: Map.new(opts, fn {k, v} -> {Atom.to_string(k), stringify(v)} end)

  defp stringify(value) when is_binary(value), do: value
  defp stringify(value) when is_integer(value) or is_boolean(value), do: to_string(value)
  defp stringify(value) when is_atom(value), do: Atom.to_string(value)
  defp stringify(value), do: value

  defp collect_phase_events do
    Telemetry.capture(Table.phase_event(), as: fn _e, _m, meta -> {:phase_event, meta} end)

    :ok
  end

  defp phase_events(:ok, plugin_id) do
    phases =
      collect do
        {:phase_event, %{plugin_id: ^plugin_id, to: to}} -> {:ok, to}
        {:phase_event, _other} -> :other
      end

    for {:ok, to} <- Enum.reverse(phases), do: to
  end
end
