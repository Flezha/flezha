defmodule Mix.Tasks.Flezha.AiKitTest do
  @moduledoc """
  `priv/ai_kit/` against its sources: the committed tree is fresh, and a copy
  with a file missing or changed is named stale.
  """
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Mix.Tasks.Flezha.AiKit

  test "priv/ai_kit is the fresh generation of what git tracks of its sources", %{tmp: tmp} do
    assert AiKit.stale() == [], "priv/ai_kit is stale: run `mix docs`"

    File.mkdir_p!(Path.join(tmp, "examples/starter"))
    File.write!(Path.join(tmp, "examples/starter/README.md"), "# Starter: a relay\n")
    {_, 0} = System.cmd("git", ~w(-C #{tmp} init -q))
    {_, 0} = System.cmd("git", ~w(-C #{tmp} add examples))
    File.write!(Path.join(tmp, "examples/starter/notes.txt"), "a working note\n")

    assert AiKit.tracked(tmp) == ["examples/starter/README.md"]
  end

  test "every SDK has its example and every page the map links is there", %{tmp: tmp} do
    File.cp_r!("priv/ai_kit", tmp)

    for language <- Flezha.Mcp.Knowledge.languages(),
        do: assert(File.exists?(Path.join(tmp, "sdk/#{language}.md")))

    File.rm!(Path.join(tmp, "docs/running_and_testing.md"))
    File.write!(Path.join(tmp, "sdk/python.md"), "an older form\n", [:append])
    assert AiKit.stale(tmp) == ["docs/running_and_testing.md", "sdk/python.md"]
  end

  test "every SDK's starter is copied, and a changed or extra copy is stale", %{tmp: tmp} do
    File.cp_r!("priv/ai_kit", tmp)
    manifest = Jason.decode!(File.read!(Path.join(tmp, "templates/manifest.json")))

    assert for(t <- manifest["templates"], do: {t["language"], t["example"]}) ==
             for(language <- Flezha.Mcp.Knowledge.languages(), do: {language, "starter"})

    File.write!(Path.join(tmp, "templates/java/starter/pom.xml"), "<!-- edited -->\n", [:append])
    File.write!(Path.join(tmp, "templates/rust/starter/notes.txt"), "a stray file\n")

    assert AiKit.stale(tmp) == [
             "templates/java/starter/pom.xml",
             "templates/rust/starter/notes.txt"
           ]
  end

  test "a template's summary is its README's title after the colon, or a refusal naming it" do
    assert AiKit.summary("# Starter: a relay that counts\n\nMore.", "x/") == "A relay that counts"

    assert_raise Mix.Error, ~s(x/README.md's title has no ": " before its summary), fn ->
      AiKit.summary("# Starter\n", "x/")
    end
  end

  test "the map leaves out what the knowledge tools do not serve" do
    llms = """
    ## Start here

    - [Local mode](local_mode.md)
    - [Operation catalogue](catalogue.json): every published operation

    ## Reference

    - [Errors](errors.md)

    ## Published API

    - [Flezha.Error](Flezha.Error.md): The one shape.
    """

    map = AiKit.map(llms)

    assert map =~ "(local_mode.md)"
    assert map =~ "(errors.md)"
    refute map =~ "catalogue.json"
    refute map =~ "Published API"
    refute map =~ "Flezha.Error.md"
  end
end
