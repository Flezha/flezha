defmodule Mix.Tasks.Flezha.LlmsTest do
  @moduledoc """
  The assistant map `mix flezha.llms` writes over ExDoc's `llms.txt`, built from
  a fixture `doc/` in a temp directory: what it lists, what it leaves out, and
  its refusals.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  import ExUnit.CaptureIO

  alias Mix.Tasks.Flezha.Catalogue
  alias Mix.Tasks.Flezha.Llms

  @published Flezha.Service.modules() ++ [Flezha.Error]

  @exdoc_llms """
  # Flezha v0.1.0 - Table of Contents

  > Distributed stateful relays

  ## Pages

  - Getting started
    - [What is Flezha](what_is_flezha.md)

  - Core topics
    - [Running and testing](running_and_testing.md)

  - The CLI
    - [The CLI](cli.md)

  - Architecture
    - [The kernel](kernel.md)

  - Reference
    - [Errors](errors.md)

  - A new group
    - [A new page](new_page.md)

  - Contributing
    - [Setting up](setting_up.md)

  ## Modules

  - [Flezha.AuthN.Cert](Flezha.AuthN.Cert.md): Certificates.

  ## Mix Tasks

  - [mix bench.tally](Mix.Tasks.Bench.Tally.md): Tallies a bench run.
  """

  @pages %{
    "what_is_flezha.md" => """
    # What is Flezha

    Flezha is a durable, distributed runtime for stateless and stateful data processing.

    It runs many small, independent pieces of work in parallel.
    """,
    "running_and_testing.md" => """
    # Running and testing

    <!-- A comment
    that spans lines. -->

    **Contents**

    1. [Start](#start)

    Local mode runs a relay inside your
    test, with no controller. It is fast.
    """,
    "cli.md" => """
    # The CLI

    ```shell
    flz help
    ```

    `flz` is the command line. Each verb takes `--json`.
    """,
    "kernel.md" => "# The kernel\n\nThe kernel is inside.\n",
    "errors.md" =>
      "# Errors\n\n<!-- Generated. -->\n\n| a | b |\n\nEvery failure carries one shape.\n",
    "new_page.md" => "# A new page\n\n## A section\n\nNot the introduction.\n",
    "setting_up.md" => "# Setting up\n\nClone the repository.\n"
  }

  # A module copy as ExDoc writes it: its name and source link, then the moduledoc.
  defp module_copy(module),
    do:
      "# `#{inspect(module)}`\n[🔗](https://example.com)\n\nThe #{inspect(module)} surface. More.\n"

  setup %{tmp: tmp} do
    File.write!(Path.join(tmp, "llms.txt"), @exdoc_llms)
    for {file, text} <- @pages, do: File.write!(Path.join(tmp, file), text)

    for module <- @published,
        do: File.write!(Path.join(tmp, "#{inspect(module)}.md"), module_copy(module))

    %{doc: tmp}
  end

  test "the map lists the user-facing pages and the published API, nothing internal", %{doc: doc} do
    map = Llms.render(doc)

    refute map =~ "Mix.Tasks.Bench"
    refute map =~ "Flezha.AuthN.Cert"
    refute map =~ "kernel.md"
    refute map =~ "setting_up.md"
    refute map =~ "## Pages"

    assert map =~ "> Distributed stateful relays\n"
    assert map =~ "## A new group\n\n- [A new page](new_page.md)\n"

    [_, api] = Regex.run(~r/## Published API\n\n(.*)\z/s, map)

    assert api ==
             Enum.map_join(
               @published,
               &"- [#{inspect(&1)}](#{inspect(&1)}.md): The #{inspect(&1)} surface.\n"
             )
  end

  test "docs/public holds exactly the pages the two ExDoc sites publish" do
    extras =
      for config <- [Mix.Project.config()[:docs], Flezha.MixProject.tutorial_docs(nil)],
          extra <- config[:extras],
          is_binary(extra),
          do: extra

    unpublished = Path.wildcard("docs/public/**/*.md") -- extras
    outside = Enum.reject(extras, &String.starts_with?(&1, "docs/public/"))

    assert unpublished == [],
           "pages under docs/public/ that no extra of mix.exs publishes: #{inspect(unpublished)}"

    assert outside == [], "extras of mix.exs outside docs/public/: #{inspect(outside)}"
  end

  test "the API site leaves out the modules compiled from test/" do
    keep? = Mix.Project.config()[:docs][:filter_modules]

    assert keep?.(Flezha.Error, %{})
    refute keep?.(FlezhaTest.Case, %{})
  end

  test "each page line carries the first sentence of its introduction", %{doc: doc} do
    map = Llms.render(doc)

    assert map =~
             "- [What is Flezha](what_is_flezha.md): Flezha is a durable, distributed runtime " <>
               "for stateless and stateful data processing.\n"

    assert map =~
             "- [Running and testing](running_and_testing.md): Local mode runs a relay inside your test, " <>
               "with no controller.\n"

    assert map =~ "- [The CLI](cli.md): `flz` is the command line.\n"
    assert map =~ "- [Errors](errors.md): Every failure carries one shape.\n"
  end

  test "an abbreviation does not end the sentence", %{doc: doc} do
    File.write!(
      Path.join(doc, "cli.md"),
      "# The CLI\n\nUse a sink, e.g. Kafka, for output. More.\n"
    )

    assert Llms.render(doc) =~
             "- [The CLI](cli.md): Use a sink, e.g. Kafka, for output.\n"
  end

  test "the start list links the essentials, in the order a newcomer needs them", %{doc: doc} do
    [_, start] = Regex.run(~r/## Start here\n\n(.*?)\n\n/s, Llms.render(doc))

    assert start ==
             Enum.join(
               [
                 "- [What is Flezha](what_is_flezha.md)",
                 "- [Running and testing](running_and_testing.md)",
                 "- [The CLI](cli.md)",
                 "- [Errors](errors.md)",
                 "- [Operation catalogue](catalogue.json): every published operation " <>
                   "and its arguments, as JSON"
               ],
               "\n"
             )
  end

  test "a listed page without its Markdown copy fails, naming the page", %{doc: doc} do
    File.rm!(Path.join(doc, "cli.md"))
    assert_raise Mix.Error, ~r/cli\.md/, fn -> Llms.render(doc) end
  end

  test "a published module without its Markdown page fails, naming it", %{doc: doc} do
    File.rm!(Path.join(doc, "Flezha.Error.md"))
    assert_raise Mix.Error, ~r/Flezha\.Error\.md/, fn -> Llms.render(doc) end
  end

  test "a start page missing from the guide groups fails, naming it", %{doc: doc} do
    File.write!(
      Path.join(doc, "llms.txt"),
      String.replace(@exdoc_llms, "  - [Running and testing](running_and_testing.md)\n", "")
    )

    assert_raise Mix.Error, ~r/running_and_testing\.md/, fn -> Llms.render(doc) end
  end

  test "a page in no group fails, naming it", %{doc: doc} do
    File.write!(
      Path.join(doc, "llms.txt"),
      String.replace(@exdoc_llms, "## Pages\n", "## Pages\n\n- [Loose](loose.md)\n")
    )

    assert_raise Mix.Error, ~r/loose\.md/, fn -> Llms.render(doc) end
  end

  test "a map over 32 KB fails, naming its size", %{doc: doc} do
    pages =
      for n <- 1..300 do
        File.write!(Path.join(doc, "p#{n}.md"), "# P\n\n#{String.duplicate("word ", 40)}end.\n")
        "  - [Page #{n}](p#{n}.md)\n"
      end

    llms = String.replace(@exdoc_llms, "## Modules", "- Many\n#{pages}\n## Modules")
    File.write!(Path.join(doc, "llms.txt"), llms)

    assert_raise Mix.Error, ~r/\d+ bytes/, fn -> Llms.render(doc) end
  end

  test "a map already rewritten is refused instead of read as ExDoc's", %{doc: doc} do
    File.write!(Path.join(doc, "llms.txt"), Llms.render(doc))
    assert_raise Mix.Error, ~r/mix docs/, fn -> Llms.render(doc) end
  end

  test "the task writes the map and the catalogue into the folder it is given", %{doc: doc} do
    capture_io(fn -> Llms.run([doc]) end)

    assert File.read!(Path.join(doc, "llms.txt")) =~ "## Start here"

    assert File.read!(Path.join(doc, "catalogue.json")) ==
             File.read!(Catalogue.path())
  end
end
