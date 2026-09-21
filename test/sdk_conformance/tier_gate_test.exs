defmodule FlezhaTest.SdkConformance.TierGateTest do
  @moduledoc """
  The tier table and the four places a tier is selected from.

  A tier is a slice of the suite plain `mix test` does not run.
  `FlezhaTest.Tiers` declares one; `test/test_helper.exs` derives `exclude:`
  from it; a workflow, a script, a module's own moduledoc and
  `docs/sdk/conformance.md` each select one by name. Before the table those
  five places were edited by hand, and they disagreed: three of nine tiers were
  in no document, six were run by no workflow, and ten tag names shaped like
  selectors selected nothing at all.

  Untagged on purpose, like the gate beside it: it rides plain `mix test` on
  every pull request, so a tier that stops being run or stops being described is
  caught by the change that does it.
  """

  use ExUnit.Case, async: true

  alias FlezhaTest.Helper.SourceWalk
  alias FlezhaTest.Tiers

  @system_dir "test/system"
  @docs_section "docs/sdk/conformance.md § The test tiers"

  # The four places a tier is selected from. `docs/sdk/conformance.md` is in the
  # list because a documented command a reader copies is as much a reader of a
  # tier name as a workflow is.
  @tier_selector_roots ["test", ".github", "scripts", "docs/sdk/conformance.md"]

  # This file spells the shapes it looks for, as data.
  @tier_self "test/sdk_conformance/tier_gate_test.exs"

  describe "test tiers" do
    test "every selector names a declared tier" do
      for {path, line, token} <- tier_selectors(), token not in tier_tag_strings() do
        fail(
          "#{path}:#{line} selects `#{token}`, which `FlezhaTest.Tiers` does not declare. " <>
            "Declare it as a row, or stop selecting it."
        )
      end
    end

    # A name shaped like a filter is read as one. Nine `*_e2e` names were spelled
    # across fourteen sites and selected by no workflow, no script and no `mix`
    # alias, beside a `capability:` vocabulary that IS read: the tag looked like
    # the way to run a scenario and ran nothing.
    test "no tag is spelled like a tier without being one" do
      for {path, line, tag} <- selector_shaped_tags(), tag not in tier_tag_strings() do
        fail(
          "#{path}:#{line} tags `:#{tag}`, whose name reads as a tier selector, and " <>
            "`FlezhaTest.Tiers` does not declare it. Either declare the tier, or drop the " <>
            "tag: `use FlezhaTest.SystemCase, capability: :x` already carries the axis."
        )
      end
    end

    test "every declared tier is run by a workflow or says why a person runs it" do
      for tier <- Tiers.all() do
        case tier do
          %{runs_in: :workflow, workflow: file} when is_binary(file) ->
            path = Path.join(Tiers.workflow_dir(), file)

            assert File.exists?(path), "#{tier.tag} names #{file}, which does not exist"

            source = File.read!(path)

            # A subset runs whenever its parent's include runs, so the workflow
            # line naming the parent is what proves the subset runs too.
            selected =
              [tier.tag | List.wrap(tier.within)]
              |> Enum.map(&"--include #{&1}")
              |> Enum.any?(&String.contains?(source, &1))

            assert selected,
                   "#{tier.tag} names #{file}, whose `mix test` line includes neither " <>
                     "#{tier.tag} nor its parent #{inspect(tier.within)}"

            if tier.within do
              parent = Tiers.fetch(tier.within)

              assert parent != nil and parent.runs_in == :workflow,
                     "#{tier.tag} is a subset of #{tier.within}, which no workflow runs"
            end

          %{runs_in: :manual, workflow: nil, reason: reason} when is_binary(reason) ->
            :ok

          other ->
            fail(
              "#{other.tag} declares neither a workflow that runs it nor `runs_in: :manual` " <>
                "with a reason, so it is a slice of the suite nothing anywhere runs"
            )
        end
      end
    end

    test "every declared tier tags at least one module" do
      tagged = MapSet.new(all_tags(), & &1)

      for tier <- Tiers.all(), not MapSet.member?(tagged, Atom.to_string(tier.tag)) do
        fail("#{tier.tag} is declared and no test module carries the tag")
      end
    end

    # The published table is gated against the declared one rather than
    # generated from it: a document a reader edits by hand is where a tier
    # stopped being described, so the check is that every row reaches the page.
    test "every declared tier points at a section of the conformance page that names it" do
      page = File.read!("docs/sdk/conformance.md")

      for tier <- Tiers.all() do
        assert String.contains?(page, tier.doc),
               "#{tier.tag} names the section #{inspect(tier.doc)}, which conformance.md lacks"

        assert String.contains?(page, "`:#{tier.tag}`"),
               "#{tier.tag} is declared and docs/sdk/conformance.md never names it"
      end
    end

    # `blocks_merge` is derived from the workflow's own triggers, so the column
    # the conformance page publishes is checked against it rather than
    # maintained beside it. A trigger change moves the derived value and the
    # page then disagrees, which is the point: the fact "this mechanism is
    # proven only nightly" cannot go stale in silence.
    test "the published blocks-merge column is the derived one" do
      rows =
        "docs/sdk/conformance.md"
        |> File.read!()
        |> String.split("\n")
        |> Enum.flat_map(fn line ->
          case Regex.run(~r/^\| `:([a-z_0-9]+)` \| [^|]+ \| (yes|no) \|/, line) do
            [_, tag, published] -> [{String.to_atom(tag), published == "yes"}]
            nil -> []
          end
        end)

      assert length(rows) == length(Tiers.all()),
             "conformance.md's tier table has #{length(rows)} rows for " <>
               "#{length(Tiers.all())} declared tiers"

      for {tag, published} <- rows do
        assert Tiers.blocks_merge(tag) == published,
               "conformance.md says #{tag} blocks merge: #{published}, but the workflow " <>
                 "#{inspect(Tiers.fetch(tag).workflow)} says #{Tiers.blocks_merge(tag)}"
      end
    end

    test "the suite's exclude list is the table's" do
      assert Enum.sort(ExUnit.configuration()[:exclude] -- [:test]) ==
               Enum.sort(Tiers.excluded()),
             "test_helper.exs no longer derives `exclude:` from FlezhaTest.Tiers"
    end

    # Selection is proven the way ExUnit itself decides it: `mix test` has no
    # dry run, so the tag map a module's `@moduletag`s produce is run through
    # the same filter evaluation the runner uses.
    test "an include selects its tier's modules and an undeclared tag selects nothing" do
      exclude = Tiers.excluded()

      for tier <- Tiers.all(), tier.excluded do
        tags = %{tier.tag => true, :module => "M", :test => :t, :describe => nil}

        assert ExUnit.Filters.eval([tier.tag], exclude, tags, []) == :ok,
               "--include #{tier.tag} does not select a module tagged #{tier.tag}"

        assert match?({:skipped, _}, ExUnit.Filters.eval([], exclude, tags, [])) or
                 match?({:excluded, _}, ExUnit.Filters.eval([], exclude, tags, [])),
               "a module tagged #{tier.tag} is not excluded from the default run"
      end

      undeclared = %{flezha_no_such_tier: true, module: "M", test: :t, describe: nil}

      assert match?(
               {:excluded, _},
               ExUnit.Filters.eval(
                 [:flezha_no_such_tier],
                 exclude,
                 %{
                   system: true,
                   module: "M",
                   test: :t,
                   describe: nil
                 },
                 []
               )
             ),
             "an undeclared tag selects a tier module, so a typo would run something"

      assert ExUnit.Filters.eval([:flezha_no_such_tier], exclude, undeclared, []) == :ok
    end

    test "the system tier's declared transports are driven by at least one module" do
      modules = Path.wildcard(Path.join(@system_dir, "*_test.exs"))

      fanned =
        Enum.count(modules, &String.contains?(SourceWalk.read(&1), "NodeTestHelper.transports"))

      assert Tiers.fetch(:system).transports == [:uds, :quic]

      # The ratio is measured, never pinned: it moves whenever a scenario is
      # added, and a number asserted here would be edited rather than read. What
      # has to hold is that the declared pair is driven at all, because a tier
      # declaring two transports nothing fans out over is a claim nobody checks.
      # The census rides the failure, which is where a reader needs it.
      assert fanned > 0,
             "#{length(modules)} system modules, and not one fans out over " <>
               "`NodeTestHelper.transports/0`: the declared transport pair drives nothing"
    end
  end

  defp fail(message), do: flunk(message <> "\n\nSee " <> @docs_section <> ".")

  # ---------- tier readers ----------

  defp tier_tag_strings, do: Enum.map(Tiers.tags(), &Atom.to_string/1)

  # Every `--include <tag>` / `--only <tag>` in the four roots, with where it is.
  defp tier_selectors do
    selector = ~r/(?:--include|--only)\s+([a-z_0-9]+)/

    for path <- tier_readable_files(),
        String.contains?(SourceWalk.read(path), ["--include", "--only"]),
        {_path, line, lineno} <- SourceWalk.lines([path]),
        [_, token] <- Regex.scan(selector, line),
        do: {path, lineno, token}
  end

  # A bare `@tag :name` / `@moduletag :name` whose name reads as a tier
  # selector. `_e2e` is the suffix the tree used for exactly that.
  defp selector_shaped_tags do
    e2e_tag = ~r/@(?:module)?tag\s+:([a-z_0-9]+_e2e)\s*$/

    for path <- SourceWalk.files(["test"], [".exs"]),
        String.contains?(SourceWalk.read(path), "_e2e"),
        {_path, line, lineno} <- SourceWalk.lines([path]),
        [_, tag] <- Regex.scan(e2e_tag, line),
        do: {path, lineno, tag}
  end

  defp all_tags do
    tag = ~r/@(?:module)?tag\s+(?::([a-z_0-9]+)\s*$|([a-z_0-9]+): true)/m

    for path <- SourceWalk.files(["test"], [".exs"]),
        [_, name] <-
          tag
          |> Regex.scan(SourceWalk.read(path))
          |> Enum.map(fn parts -> [hd(parts), Enum.find(tl(parts), &(&1 != ""))] end),
        uniq: true,
        do: name
  end

  defp tier_readable_files, do: List.delete(SourceWalk.tree(@tier_selector_roots), @tier_self)
end
