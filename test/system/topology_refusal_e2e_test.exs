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

  Plus the boot drill: a runner started under `FLEZHA_REFUSAL_FIXTURE=<row>`
  registers only that row's topology and must die with the row's refusal name in
  its stderr. A runner that refuses at boot never connects, so the assertion
  reads the app's exit status and stderr rather than a control frame.

  Tagged `:interop` as well as `:system`: the runtimes come from the manifests,
  so every test app must be built. It runs in the nightly's interop job.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle
  use Mimic

  alias Flezha.Service.PortPilot
  alias FlezhaTest.Helper.DeclarationSnapshot
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ReferenceTopologies

  @moduletag interop: true
  @moduletag timeout: 300_000

  @moduletag skip:
               FlezhaTest.SystemCase.skip_unless(
                 :interop in (ExUnit.configuration()[:include] || []),
                 "the drill boots every runtime with a manifest; pass --include interop"
               )

  @fixture "test/fixtures/reference_topologies.txt"

  # A row needing one of these is authorable in no builder: a fluent handle
  # cannot name a node nothing wired, and every SDK's `outputs_at_checkpoint`
  # takes the delay its flag needs. The fixture header states both; they are
  # skipped everywhere and counted as skips, never as holes.
  @unauthorable ~w(detached raw_flags raw_class_flags)

  describe "every runtime reads the whole refusal table" do
    for runtime <- PortPilot.runtimes() do
      @runtime runtime

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

      test "#{runtime} refuses every malformed row by the fixture's name" do
        report = conformance_report(@runtime)
        rows = ReferenceTopologies.refusal_rows()

        for entry <- report["rows"], entry["outcome"] == "refused" do
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

        refused = for e <- report["rows"], e["outcome"] == "refused", do: e["topology"]
        skipped = for e <- report["rows"], e["outcome"] == "skipped", do: e["topology"]

        for row <- rows, row.refusal != "ok", row.topology not in skipped do
          assert row.topology in refused,
                 "#{@runtime} accepted #{row.topology}, which every SDK must refuse " <>
                   "with #{row.refusal} — this is the KI-118 shape"
        end
      end

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
      @describetag capability: capability,
                   skip: FlezhaTest.SystemCase.deferral(capability)

      @factories factories

      for runtime <- PortPilot.runtimes() do
        @runtime runtime

        test "#{runtime}'s #{capability} declarations equal their goldens", ctx do
          NodeTestHelper.start_runner(ctx.namespace, runtime: @runtime)

          for factory <- @factories, factory in declared_by(@runtime) do
            DeclarationSnapshot.assert_matches_golden(
              ctx.namespace,
              factory,
              kind_of(factory),
              &"#{&1}_#{@runtime}_#{System.unique_integer([:positive])}"
            )
          end
        end
      end
    end
  end

  describe "the boot drill" do
    for runtime <- PortPilot.runtimes() do
      @runtime runtime

      test "#{runtime} dies at boot naming the refusal it was given" do
        rows =
          ReferenceTopologies.refusal_rows()
          |> Enum.filter(&(&1.refusal != "ok" and &1.needs == "-"))

        row = Enum.find(rows, &(&1.topology == "dup_key_by")) || hd(rows)

        {output, status} = boot_with_fixture(@runtime, row.topology)

        assert status != 0,
               "#{@runtime} booted a topology the core refuses; a refused class must " <>
                 "not reach a connected runner"

        assert output =~ row.refusal,
               """
               #{@runtime} refused #{row.topology} but its runner swallowed the \
               name on the way to exit. The refusal an operator reads is the \
               verdict's own name.
               Read: #{output}
               """
      end
    end
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
             "see docs/sdk/conformance.md § Adding an SDK"

    path |> File.read!() |> Jason.decode!()
  end

  defp boot_with_fixture(runtime, topology) do
    PortPilot.launch_command(runtime, :test, "refusal_drill")
    |> run_app([{"FLEZHA_REFUSAL_FIXTURE", topology}])
  end

  defp run_app(command, env) do
    [exe | args] = String.split(command, " ", trim: true)
    System.cmd(exe, args, env: env, stderr_to_stdout: true, cd: File.cwd!())
  end

  # The factories a runtime's manifest lists across every capability it did not
  # defer: a golden for a capability an SDK defers is not that SDK's to declare.
  defp declared_by(runtime) do
    PortPilot.manifest(runtime)["fixtures"]
    |> Enum.flat_map(fn {_capability, factories} -> factories end)
    |> MapSet.new()
  end

  # A workflow golden binds as a workflow; everything else is a stream.
  defp kind_of(factory) do
    if String.contains?(factory, "workflow"), do: :workflow, else: :stream
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

  defp blank(nil), do: "-"
  defp blank(value), do: to_string(value)
  defp blank_if_empty(""), do: "-"
  defp blank_if_empty(value), do: value
end
