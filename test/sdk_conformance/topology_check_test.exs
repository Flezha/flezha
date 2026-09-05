defmodule FlezhaTest.SdkConformance.TopologyCheckTest do
  @moduledoc """
  One check, one vocabulary, one fixture — and no way past them.

  Judging a topology is one job: is this graph one the runtime can honour. It
  used to be done per SDK, in four sizes — 28 named refusals in Rust, 24 in
  Elixir, 15 in Java, 5 in each veneer — which is why adding one rule edited
  four builders six times running, and why a class that registered on one SDK
  died at boot on another. `flz_core::host::register::check` now holds it, and
  `test/fixtures/reference_topologies.txt` is what both the check and every
  SDK's conformance run are measured against.

  Three of the five clauses run here, where no runner is spawned. The other two
  — every runtime reads the whole table, and every well-formed row derives to
  the same bytes in every runtime — need a runner each and live in
  `test/system/topology_refusal_e2e_test.exs`, which reads the per-app
  conformance report this module's fixture drives.
  """

  use ExUnit.Case, async: true

  alias FlezhaTest.Helper.ReferenceTopologies

  @issues "flz_core/src/host/issue.rs"
  @fixture "test/fixtures/reference_topologies.txt"
  @decl_keys "test/fixtures/host_class_decl_keys.txt"
  @record "flz_core/src/host/decl.rs"
  @seams "docs/refactor/seams.toml"

  # The residue of the text refusals the vocabulary has not named yet
  # (the language byte, the SDK version, the workflow ladder, the DAG leaf, the
  # index arities). It is deliberately the ONE variant with no fixture row: a
  # declaration is never refused with no verdict at all, and every refusal that
  # earns a name earns a row with it.
  @unnamed_residue "Declaration"

  describe "the vocabulary and the fixture are one set" do
    test "every refusal the core can raise has a row, and every row names one" do
      variants = variants()
      rows = ReferenceTopologies.refusal_rows()

      assert length(variants) > 25,
             "the vocabulary should be the reference set, read #{length(variants)} variants"

      missing_rows = variants -- ([@unnamed_residue] ++ Enum.map(rows, & &1.refusal))

      assert missing_rows == [],
             """
             a TopologyIssue variant with no fixture row can be raised and never \
             driven by any SDK. Add one row per name to #{@fixture}:
             #{Enum.join(missing_rows, "\n")}
             """

      known = MapSet.new(["ok", @unnamed_residue | variants])

      unknown =
        rows |> Enum.map(& &1.refusal) |> Enum.reject(&MapSet.member?(known, &1)) |> Enum.uniq()

      assert unknown == [],
             """
             a fixture row naming no variant is a row no runtime can satisfy. \
             Remove it, or add the variant to #{@issues}:
             #{Enum.join(unknown, "\n")}
             """
    end

    test "every row spells a topology id and a needs column" do
      for row <- ReferenceTopologies.refusal_rows() do
        assert row.topology =~ ~r/^[a-z][a-z0-9_]*$/,
               "row #{row.refusal} needs a snake_case topology id, read #{inspect(row.topology)}"

        assert row.steps != "", "row #{row.refusal} declares no authoring steps"

        assert row.needs in ReferenceTopologies.known_surfaces(),
               """
               row #{row.refusal} needs surface #{inspect(row.needs)}, which the \
               fixture header does not declare. A skip is only countable against \
               a surface the header names.
               """
      end
    end

    test "a topology id is claimed by exactly one row" do
      duplicates =
        ReferenceTopologies.refusal_rows()
        |> Enum.frequencies_by(& &1.topology)
        |> Enum.filter(fn {_id, count} -> count > 1 end)
        |> Enum.map(&elem(&1, 0))

      assert duplicates == [],
             "a runtime selects a row by its topology id, so two rows cannot share one: " <>
               Enum.join(duplicates, ", ")
    end
  end

  describe "the record's key set is the pinned one" do
    test "the fixture lists exactly the keys the core deserializes" do
      pinned =
        @decl_keys
        |> File.read!()
        |> String.split("\n", trim: true)
        |> Enum.reject(&String.starts_with?(&1, "#"))
        |> MapSet.new()

      serialized = record_keys()

      assert MapSet.difference(serialized, pinned) |> MapSet.to_list() == [],
             """
             the core deserializes a key #{@decl_keys} does not list, so one \
             builder can spell it and the others cannot.
             """

      assert MapSet.difference(pinned, serialized) |> MapSet.to_list() == [],
             """
             #{@decl_keys} lists a key the core no longer deserializes; \
             deny_unknown_fields makes that a boot refusal for every SDK that \
             still writes it.
             """
    end

    test "the keys the chains replaced are gone from the record" do
      body = File.read!(@record)

      refute body =~ ~r/pub consumers:/,
             "the record carries chains now; a declared consumer list is the builder judging again"

      refute body =~ ~r/pub ack_granularity:/,
             "the granularity is derived from any checkpoint key, never declared"
    end
  end

  describe "the ledger" do
    test "the seam has a row, and the Elixir re-expression is the only allowance" do
      seams = File.read!(@seams)

      assert seams =~ ~s(id = "check-a-topology"),
             "the fold is not measured until #{@seams} carries its row"

      [_, allowed] = Regex.run(~r/id = "check-a-topology".*?^allow = \[(.*?)^\]/ms, seams)

      for file <- ~w(
            flz_core/src/host/register.rs
            flz_core/src/host/chains.rs
            flz_core/src/host/issue.rs
            lib/flezha/sdk/topology.ex
          ) do
        assert allowed =~ file,
               "#{file} is the seam or its re-expression and must be allow-listed"
      end

      # Only the Elixir SDK re-expresses the check; every other SDK submits to
      # it, so an allowance for one would be the scatter growing back.
      for sdk <- ~w(sdk/rust sdk/java sdk/node sdk/python sdk/ruby sdk/php) do
        refute allowed =~ sdk, "#{sdk} submits to the seam; it may not be allow-listed out of it"
      end
    end
  end

  # The variant names, read from the enum the way the scatter map's own command
  # reads them: four-space-indented CamelCase at the start of a variant.
  defp variants do
    @issues
    |> File.read!()
    |> then(&Regex.scan(~r/^    ([A-Z][A-Za-z]+)/m, &1))
    |> Enum.map(fn [_, name] -> name end)
    |> Enum.uniq()
  end

  # The keys `HostClassDecl` and its nested declarations deserialize, read off
  # the serde struct fields the same way the fixture spells them.
  defp record_keys do
    body = File.read!(@record)

    for {struct, section} <- [
          {"HostClassDecl", "class"},
          {"HostChannelDecl", "channel"},
          {"HostProcedureDecl", "procedure"},
          {"HostWorkflowDecl", "workflow"}
        ],
        key <- struct_fields(body, struct),
        into: MapSet.new() do
      "#{section}.#{key}"
    end
  end

  defp struct_fields(body, struct) do
    [_, fields] = Regex.run(~r/pub struct #{struct} \{(.*?)\n\}/s, body)
    Regex.scan(~r/^    pub ([a-z_]+):/m, fields) |> Enum.map(fn [_, name] -> name end)
  end
end
