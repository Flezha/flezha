defmodule FlezhaTest.System.TopologyRefusalE2ETest do
  @moduledoc """
  The two clauses of the topology-check seam that need a runner each.

  `test/fixtures/reference_topologies.txt` names one refusal per row. This
  module drives every row through every runtime that has a test-app manifest,
  and every committed declaration golden through every runtime — not through
  the one runtime `FLEZHA_E2E_RUNTIME` happens to select.

  Two tables, both read from the fixture and the manifests, so a new refusal or
  a new SDK is covered with no edit here:

  1. **Every runtime reads the whole table.** Each app writes its conformance
     report with `--conformance-report <path>`: per row, refused with which
     names, accepted with which derived declaration, or skipped naming the
     surface its builder lacks. Ran + skipped must equal the table, and every
     skip must name a surface the runtime's manifest declares absent.
  2. **Every well-formed row derives the same bytes.** An `ok` row's derived
     declaration is compared to the fixture's seven-column lines, in every
     runtime, and each golden under `test/fixtures/declarations/` is compared to
     what the controller decodes from every runtime — the smoke tier's snapshot
     tests generalised from one runtime per run to all of them.

  Plus the announced graph: each runtime's runner is started and the graph the
  controller keeps for the two reference classes is compared to the fixture's
  seven-column lines.

  And the boot drill: a runner started under `FLEZHA_REFUSAL_FIXTURE=<row>`
  registers only that row's topology and must die with the row's refusal name in
  its stderr. A runner that refuses at boot never connects, so the assertion
  reads the app's exit status and stderr rather than a control frame.

  Tagged `:interop` as well as `:system`: the whole sweep reads the runtimes
  from the manifests, so it needs every test app built and only a run that
  passes `--include interop` takes it. The exception is the SELECTED runtime,
  `FLEZHA_E2E_RUNTIME`, the one a nightly job builds, whose clauses also run in
  the ordinary `:system` pass. That is what makes the drill run in CI at all:
  the nightly is one job per runtime and no job builds a second one, so a
  steps relay line asking for the whole sweep would be red on every runtime it did
  not build.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle
  use Mimic

  import FlezhaTest.Helper.TierE2EHarness, only: [stop_namespace: 1, unique: 1]

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.DeclarationSnapshot
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ReferenceTopologies

  @moduletag interop: true
  @moduletag timeout: 300_000

  # A `skip:` tag on a describe or a test replaces this one rather than adding
  # to it, so every narrower `skip:` below restates it.
  @interop_skip FlezhaTest.Tiers.skip_unless_included(:interop)
  @moduletag skip: @interop_skip

  @fixture "test/fixtures/reference_topologies.txt"

  # A graph's channel kind as the record spells it.
  @record_kinds %{"inter_tasklet" => "inter_task", "plugin_bound" => "plugin"}

  # How a row's refusal may be answered: driven at run time, or cited to the
  # compile-fail test that pins the message where the runtime's compiler
  # refuses it before a runner exists.
  @answered ["refused", "cited"]

  # A row needing one of these is authorable in no builder: a fluent handle
  # cannot name a node nothing wired, and every SDK's `outputs_at_checkpoint`
  # takes the delay its flag needs. The fixture header states both; they are
  # skipped everywhere and counted as skips, never as holes.
  @unauthorable ~w(detached raw_flags)

  describe "every runtime reads the whole refusal table" do
    for runtime <- PortPilot.runtimes() do
      @runtime runtime

      # The selected runtime is the one this run has built, so its clauses need
      # no second app and ride the ordinary `:system` pass; every other runtime
      # waits for a run that includes `:interop`.
      @selected_only runtime != NodeTestHelper.runtime() and @interop_skip

      @tag skip: @selected_only
      test "#{runtime} ran or justified every row" do
        report = conformance_report(@runtime)
        rows = ReferenceTopologies.refusal_rows()

        accounted = Enum.map(report["rows"], & &1["topology"]) |> MapSet.new()
        expected = Enum.map(rows, & &1.topology) |> MapSet.new()

        assert MapSet.difference(expected, accounted) |> MapSet.to_list() == [],
               """
               #{@runtime} silently dropped a row. A row the interpreter neither \
               ran nor skipped is a refusal that SDK is not measured against.
               """

        for entry <- report["rows"], entry["outcome"] == "skipped" do
          row = Enum.find(rows, &(&1.topology == entry["topology"]))

          assert entry["needs"] == row.needs,
                 "#{@runtime} skipped #{row.topology} naming #{inspect(entry["needs"])}, " <>
                   "but the fixture says it needs #{inspect(row.needs)}"

          assert row.needs != "-",
                 "#{@runtime} skipped #{row.topology}, which needs no surface at all"

          assert row.needs in @unauthorable or row.needs not in surfaces(@runtime),
                 "#{@runtime} skipped #{row.topology} for #{row.needs}, which its " <>
                   "manifest declares it carries"
        end
      end

      @tag skip: @selected_only
      test "#{runtime} refuses every malformed row by the fixture's name" do
        report = conformance_report(@runtime)
        rows = ReferenceTopologies.refusal_rows()

        for entry <- report["rows"], entry["outcome"] in @answered do
          row = Enum.find(rows, &(&1.topology == entry["topology"]))

          assert row.refusal != "ok",
                 "#{@runtime} refused #{row.topology}, which the fixture calls well-formed"

          assert row.refusal in entry["names"],
                 """
                 #{@runtime} refused #{row.topology} with #{inspect(entry["names"])} \
                 and the fixture names #{row.refusal}. One check, one vocabulary: \
                 a refusal reaches an author under the same name in every language.
                 """
        end

        # A refusal the runtime's compiler answers cannot be driven at run time,
        # so the row is answered by citing the compile-fail test that pins the
        # message. The citation is checked here rather than taken on trust.
        for entry <- report["rows"], entry["outcome"] == "cited" do
          cited = entry["citation"]

          assert is_binary(cited) and cited != "",
                 "#{@runtime} cited #{entry["topology"]} without naming the test that pins it"

          assert File.exists?(cited),
                 "#{@runtime} cites #{cited} for #{entry["topology"]}, which is not in the tree"

          name = hd(entry["names"])

          assert File.read!(cited) =~ name,
                 """
                 #{@runtime} cites #{cited} for #{entry["topology"]}, and that file does not \
                 carry #{name}. A citation nothing checks is a refusal nobody holds.
                 """
        end

        answered =
          for e <- report["rows"], e["outcome"] in @answered, do: e["topology"]

        skipped = for e <- report["rows"], e["outcome"] == "skipped", do: e["topology"]

        for row <- rows, row.refusal != "ok", row.topology not in skipped do
          assert row.topology in answered,
                 "#{@runtime} accepted #{row.topology}, which every SDK must refuse " <>
                   "with #{row.refusal} — this is the KI-118 shape"
        end
      end

      @tag skip: @selected_only
      test "#{runtime} derives the same bytes for every well-formed row" do
        report = conformance_report(@runtime)

        for entry <- report["rows"], entry["outcome"] == "accepted" do
          expected = ReferenceTopologies.derived_lines(entry["topology"])

          if expected != [] do
            assert normalise(entry["derived"]) == expected,
                   """
                   #{@runtime} derived a different declaration for \
                   #{entry["topology"]} than #{@fixture} pins. A well-formed \
                   record derives to one declaration, whichever builder wrote it.
                   """
          end
        end
      end
    end
  end

  # One describe per capability the goldens belong to, so a capability is proved
  # by a scenario tagged for it and a runtime that defers one skips its row —
  # the same rule the smoke tier's snapshot tests carried before they folded
  # here, now over every runtime rather than the one a run selected.
  for {capability, factories} <- DeclarationSnapshot.by_capability() do
    describe "declared surface: #{capability}" do
      @describetag capability: capability

      setup :own_namespace

      @factories factories

      for runtime <- PortPilot.runtimes() do
        @runtime runtime

        # The selected runtime needs no second one built, so it runs in the
        # ordinary pass too; every other runtime waits for the interop tier.
        # Each test binds one runtime, so the deferral that skips it is that
        # runtime's own.
        @tag skip:
               (runtime != NodeTestHelper.runtime() and @interop_skip) ||
                 FlezhaTest.SystemCase.deferral(capability, runtime)
        test "#{runtime}'s #{capability} declarations equal their goldens", ctx do
          NodeTestHelper.start_runner(ctx.namespace, runtime: @runtime)

          for factory <- @factories, factory in declared_by(@runtime) do
            DeclarationSnapshot.assert_matches_golden(
              ctx.namespace,
              factory,
              kind_of(factory),
              &unique/1
            )
          end
        end
      end
    end
  end

  # The two reference classes, by the factory id every test app registers
  # them under and the topology id the fixture pins them as. Each test is tagged
  # with its class's capability, so an SDK that defers it skips the test.
  describe "the announced graph" do
    setup :own_namespace

    for runtime <- PortPilot.runtimes() do
      @runtime runtime
      @selected_only runtime != NodeTestHelper.runtime() and @interop_skip

      @tag capability: :graph_builder,
           skip: @selected_only || FlezhaTest.SystemCase.deferral(:graph_builder, runtime)
      test "#{runtime} announces the fixture's graph for hybrid_channel", ctx do
        assert_announced_graph(ctx.namespace, @runtime, "hybrid_channel", "hybrid_channel")
      end

      @tag capability: :steps,
           skip: @selected_only || FlezhaTest.SystemCase.deferral(:steps, runtime)
      test "#{runtime} announces the fixture's graph for tier_commit_block_steps", ctx do
        assert_announced_graph(
          ctx.namespace,
          @runtime,
          "tier_commit_block_steps",
          "commit_block_chain"
        )
      end
    end
  end

  # `dup_key_by` is refused by the core's check. `dup_node` is refused by the
  # four veneer builders themselves, and a Node graph built at module scope
  # raises it while the module loads. `shorthand_duplicate_id` is a runtime
  # refusal in every SDK, so that row boots for real everywhere too.
  describe "the boot drill" do
    for runtime <- PortPilot.runtimes(),
        topology <- ~w(dup_key_by dup_node shorthand_duplicate_id) do
      @runtime runtime
      @topology topology
      @selected_only runtime != NodeTestHelper.runtime() and @interop_skip

      @tag skip: @selected_only
      test "#{runtime} dies at boot naming the refusal #{topology} was given" do
        row = Enum.find(ReferenceTopologies.refusal_rows(), &(&1.topology == @topology))

        {output, status} = boot_with_fixture(@runtime, row.topology)

        assert status != 0,
               "#{@runtime} booted a topology the core refuses; a refused class must " <>
                 "not reach a connected runner"

        assert output =~ row.refusal,
               """
               #{@runtime} refused #{row.topology} but its runner swallowed the \
               name on the way to exit. The refusal a user reads is the \
               verdict's own name.
               Read: #{output}
               """
      end
    end
  end

  # A namespace per test: the runner it starts is the only one its factories
  # can be placed on, so the declaration compared is that runtime's own.
  defp own_namespace(_ctx) do
    namespace = unique("declared")
    on_exit(fn -> stop_namespace(namespace) end)
    %{namespace: namespace}
  end

  # Each app writes its report once per run; the drill reads it rather than
  # re-driving the table per assertion.
  defp conformance_report(runtime) do
    path =
      Path.join(
        System.tmp_dir!(),
        "flezha_conformance_#{runtime}_#{System.unique_integer([:positive])}.json"
      )

    # The path rides the environment, not argv: one manifest launches through
    # `sh -c '<script>'`, where an appended word binds to `$0` inside the script
    # rather than reaching the app. An env var reaches every launch shape, and
    # it is the mechanism the boot drill below already uses.
    {_output, status} =
      PortPilot.launch_command(runtime, :test, "conformance")
      |> run_app([{"FLEZHA_CONFORMANCE_REPORT", path}])

    assert status == 0 or File.exists?(path),
           "#{runtime}'s test app must answer --conformance-report; " <>
             "see docs/internal/sdk/conformance.md § Adding an SDK"

    path |> File.read!() |> Jason.decode!()
  end

  defp boot_with_fixture(runtime, topology) do
    PortPilot.launch_command(runtime, :test, "refusal_drill")
    |> run_app([{"FLEZHA_REFUSAL_FIXTURE", topology}])
  end

  # A manifest's launch line is a SHELL line, and one of them is
  # `sh -c '<script>'`, so it goes to a shell whole, the way
  # `PortPilot.open_port/2` spawns the same line. Splitting it on spaces breaks
  # the quoting and the app never starts.
  defp run_app(command, env) do
    System.cmd("sh", ["-c", command], env: env, stderr_to_stdout: true, cd: File.cwd!())
  end

  # The factories a runtime's manifest lists across every capability it did not
  # defer: a golden for a capability an SDK defers is not that SDK's to declare.
  defp declared_by(runtime) do
    PortPilot.manifest(runtime)["fixtures"]
    |> Enum.flat_map(fn {_capability, factories} -> factories end)
    |> MapSet.new()
  end

  # A steps relay golden binds as a steps relay; everything else is unbounded.
  defp kind_of(factory) do
    if String.contains?(factory, "steps"), do: :steps, else: :flow
  end

  # The surfaces a runtime's builder can author, declared by its manifest. A
  # skip is only countable against a surface the manifest says is absent.
  defp surfaces(runtime) do
    PortPilot.manifest(runtime)["topology_surfaces"] || []
  end

  # A report's derived channels, rendered in the fixture's seven columns so the
  # two are compared as the same statement.
  defp normalise(channels) do
    for channel <- channels || [] do
      [
        to_string(channel["index"]),
        channel["name"],
        channel["kind"],
        blank(channel["producer"]),
        channel["consumers"]
        |> List.wrap()
        |> Enum.map_join(",", &to_string/1)
        |> blank_if_empty(),
        blank(channel["sink"])
      ]
    end
  end

  defp assert_announced_graph(namespace, runtime, factory, topology) do
    NodeTestHelper.start_runner(namespace, runtime: runtime)

    assert %{graph: %{} = graph} = FleetManager.factory_graph(namespace, factory),
           "#{runtime} announced no graph for #{factory}"

    assert graph |> fixture_channels() |> normalise() ==
             ReferenceTopologies.derived_lines(topology),
           """
           #{runtime} announced a graph for #{factory} that differs from \
           #{@fixture}'s #{topology} rows. Every runtime announces one graph \
           for one class.
           """
  end

  # An announced graph in a report's channel shape: tasklets by their index,
  # the kind as the record spells it, the sink as the channel an output ends.
  defp fixture_channels(graph) do
    index = graph.tasklets |> Enum.map(& &1.name) |> Enum.with_index() |> Map.new()

    for {channel, i} <- Enum.with_index(graph.channels) do
      %{
        "index" => i,
        "name" => channel.name,
        "kind" => Map.get(@record_kinds, channel.kind, channel.kind),
        "producer" => index[channel.producer],
        "consumers" => Enum.map(channel.consumers, &index[&1]),
        "sink" => if(channel.plugin_end == "output", do: channel.name)
      }
    end
  end

  defp blank(nil), do: "-"
  defp blank(value), do: to_string(value)
  defp blank_if_empty(""), do: "-"
  defp blank_if_empty(value), do: value
end
