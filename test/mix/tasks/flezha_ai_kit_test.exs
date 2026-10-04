defmodule Mix.Tasks.Flezha.AiKitTest do
  @moduledoc """
  `priv/ai_kit/` against its sources: the committed tree is fresh, and a copy
  with a file missing or changed is named stale.
  """
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Mix.Tasks.Flezha.AiKit

  test "priv/ai_kit is the fresh generation of its sources" do
    assert AiKit.stale() == [], "priv/ai_kit is stale: run `mix docs`"
  end

  test "every SDK has its example and every page the map links is there", %{tmp: tmp} do
    File.cp_r!("priv/ai_kit", tmp)

    for language <- Flezha.Mcp.Knowledge.languages(),
        do: assert(File.exists?(Path.join(tmp, "sdk/#{language}.md")))

    File.rm!(Path.join(tmp, "docs/local_mode.md"))
    assert AiKit.stale(tmp) == ["docs/local_mode.md"]

    File.write!(Path.join(tmp, "sdk/python.md"), "an older form\n", [:append])
    assert AiKit.stale(tmp) == ["docs/local_mode.md", "sdk/python.md"]
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
