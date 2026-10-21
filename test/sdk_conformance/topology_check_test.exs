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
  alias FlezhaTest.Helper.SourceWalk

  @issues "crates/flz_core/src/host/issue.rs"
  @fixture "test/fixtures/reference_topologies.txt"
  @decl_keys "test/fixtures/host_class_decl_keys.txt"
  @record "crates/flz_core/src/host/decl.rs"

  # The seam's own shape: every spelling the tree used to judge a topology with,
  # before the fold put the verdict in one place. A file outside @seam_files
  # that matches one of these is deciding the same thing a second time.
  @shape ~r/push\(TopologyIssue::|Err\(TopologyIssue::|linkIssues\.push\(|link_issues\.append\(|@link_issues <<|linkIssues\[\] =|EdgeAlreadyWired: already feeds|amendCheckpoint\(|_amend_checkpoint\(|checkpoint_policy\(|\{:invalid_topology,/

  @roots [
    "crates/flz_core/src",
    "sdk/rust/src",
    "sdk/node/src",
    "sdk/python/src",
    "sdk/ruby/lib",
    "sdk/php/src",
    "sdk/java/flezha-sdk-api/src/main",
    "sdk/java/flezha-sdk-runtime/src/main",
    "sdk/elixir/lib"
  ]

  # The seam: every SDK, the Elixir one included, submits to it.
  @seam_files [
    "crates/flz_core/src/host/register.rs",
    "crates/flz_core/src/host/chains.rs",
    "crates/flz_core/src/host/issue.rs"
  ]

  # Sites that carry a verdict `flz_core` already reached without deciding
  # one. The list only ever shrinks: an entry that stops matching is removed.
  @carries_a_verdict []

  # The authoring contract's §5, which lists the short-form refusal names. A row
  # whose `needs` starts with `shorthand` refuses something the core never sees,
  # so it is judged against this file instead of the issue vocabulary.
  @contract "docs/internal/sdk/authoring_contract.md"

  describe "the vocabulary and the fixture are one set" do
    test "every refusal the core can raise has a row, and every row names one" do
      variants = variants()
      rows = ReferenceTopologies.refusal_rows()

      assert length(variants) > 25,
             "the vocabulary should be the reference set, read #{length(variants)} variants"

      {shorthand_rows, core_rows} = Enum.split_with(rows, &shorthand_row?/1)

      assert shorthand_rows != [],
             "the short-form refusals are part of the reference table; #{@fixture} names none"

      missing_rows =
        variants -- (ReferenceTopologies.unrowed() ++ Enum.map(core_rows, & &1.refusal))

      assert missing_rows == [],
             """
             a TopologyIssue variant with no fixture row can be raised and never \
             driven by any SDK. Add one row per name to #{@fixture}:
             #{Enum.join(missing_rows, "\n")}
             """

      known = MapSet.new(["ok" | variants])

      unknown =
        core_rows
        |> Enum.map(& &1.refusal)
        |> Enum.reject(&MapSet.member?(known, &1))
        |> Enum.uniq()

      assert unknown == [],
             """
             a fixture row naming no variant is a row no runtime can satisfy. \
             Remove it, or add the variant to #{@issues}:
             #{Enum.join(unknown, "\n")}
             """

      contract_names = contract_refusal_names()

      unlisted =
        shorthand_rows
        |> Enum.map(& &1.refusal)
        |> Enum.reject(&MapSet.member?(contract_names, &1))
        |> Enum.uniq()

      assert unlisted == [],
             """
             a short-form row is judged against #{@contract} §5, which lists \
             every short-form refusal name. These rows name something it does not:
             #{Enum.join(unlisted, "\n")}
             """

      rowless =
        contract_names
        |> MapSet.difference(MapSet.new(shorthand_rows, & &1.refusal))
        |> Enum.sort()

      assert rowless == [],
             """
             #{@contract} §5 lists a short-form refusal no fixture row drives, so \
             no SDK is held to raising it. Add a row for each:
             #{Enum.join(rowless, "\n")}
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

  describe "the seam" do
    test "judging a topology happens in one place, and it is that place" do
      matched = matching_files()

      assert matched != [],
             """
             the shape matches nothing under #{Enum.join(@roots, ", ")}, so this \
             clause measures zero sites and cannot fail. Either the seam has been \
             respelled and @shape needs updating, or a root has moved.
             """

      assert "crates/flz_core/src/host/register.rs" in matched,
             "the seam itself no longer matches its own shape; @shape is measuring something else"

      strangers = matched -- (@seam_files ++ @carries_a_verdict)

      assert strangers == [],
             """
             a file outside the seam judges a topology, which is the scatter \
             growing back: one rule would then have to be changed in two places \
             and the second is the one that gets missed.
             #{Enum.join(strangers, "\n")}
             """
    end

    test "the exception list names only sites the tree still holds" do
      stale = @carries_a_verdict -- matching_files()

      assert stale == [],
             """
             a file listed here no longer matches the shape, so the allowance \
             guards nothing and hides whatever is written there next. An \
             exception list only shrinks:
             #{Enum.join(stale, "\n")}
             """
    end
  end

  # Every file under the seam's roots whose text uses one of the spellings the
  # fold replaced. The whole text is read, comments included, because a
  # commented-out refusal is a spelling somebody is about to restore. No
  # spelling spans or anchors to a line, so the text is matched whole.
  defp matching_files do
    @roots
    |> SourceWalk.files()
    |> Enum.filter(&Regex.match?(@shape, SourceWalk.read(&1)))
    |> Enum.sort()
  end

  # A row whose surface is the short form. Its refusal is the contract's, raised
  # before a declaration ever reaches the core, so the issue vocabulary has no
  # variant for it and never should.
  defp shorthand_row?(row), do: String.starts_with?(row.needs, "shorthand")

  # The refusal names §5 lists, read off its table the way an author reads it:
  # the first column of each row, backticked.
  defp contract_refusal_names do
    section =
      @contract
      |> File.read!()
      |> String.split("### 5.2 The three refusal codes")
      |> Enum.at(1)
      |> then(&(&1 && &1 |> String.split(~r/^##/m) |> hd()))

    assert section != nil,
           "#{@contract} no longer carries the §5 refusal list the fixture rows are judged against"

    ~r/^\| `(topology\.[a-z_]+)` \|/m
    |> Regex.scan(section)
    |> Enum.map(fn [_, name] -> name end)
    |> MapSet.new()
  end

  # The variants' codes, read from the enum the way the scatter map's own command
  # reads them: four-space-indented CamelCase at the start of a variant.
  defp variants do
    @issues
    |> File.read!()
    |> then(&Regex.scan(~r/^    ([A-Z][A-Za-z]+)/m, &1))
    |> Enum.map(fn [_, name] -> "topology." <> Macro.underscore(name) end)
    |> Enum.uniq()
  end

  # The keys `HostClassDecl` and its nested declarations deserialize, read off
  # the serde struct fields the same way the fixture spells them.
  defp record_keys do
    body = File.read!(@record)

    for {struct, section} <- [
          {"HostClassDecl", "class"},
          {"HostChannelDecl", "channel"},
          {"HostTaskletDecl", "tasklet"}
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
