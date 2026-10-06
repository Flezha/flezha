defmodule Flezha.Mcp.KnowledgeTest do
  use ExUnit.Case, async: true

  alias Flezha.Mcp
  alias Flezha.Mcp.Knowledge

  test "five tools, every one read-only, the language one listing the languages" do
    tools = Enum.map(Knowledge.entries(), &Mcp.tool/1)

    assert Enum.map(tools, & &1.name) ==
             ~w(flezha_guide flezha_sdk_example flezha_error flezha_docs flezha_new_project)

    assert Enum.all?(tools, & &1.annotations.readOnlyHint)
    assert Enum.at(tools, 1).inputSchema.properties["language"].enum == Knowledge.languages()
  end

  test "the guide is the rules followed by the map of the guide pages" do
    assert {:text, guide} = Knowledge.call("Flezha.guide", [])
    assert String.starts_with?(guide, Knowledge.rules())
    assert guide =~ "(local_mode.md)"
    refute guide =~ "## Published API"
    refute guide =~ "catalogue.json"
  end

  test "the SDK example holds every tracked file of that SDK's echo" do
    assert {:text, example} = Knowledge.call("Flezha.sdk_example", ["python"])

    {listed, 0} = System.cmd("git", ["-C", "sdk/python", "ls-files", "examples/echo"])

    for file <- String.split(listed, "\n", trim: true), Path.basename(file) != ".gitignore" do
      assert example =~ "\n## #{file}\n"
      assert example =~ File.read!(Path.join("sdk/python", file))
    end
  end

  test "an error's meaning is the catalogue's" do
    assert {:ok, json} = Knowledge.call("Flezha.error", ["dlq.not_found"])
    entry = Flezha.Error.catalogue()["codes"]["dlq.not_found"]

    assert %{"code" => "dlq.not_found", "class" => class, "message" => message} =
             Jason.decode!(json)

    assert {class, message} == {entry["class"], entry["message"]}
  end

  test "a page is served by the name the map links, with or without .md" do
    assert {:text, page} = Knowledge.call("Flezha.docs", ["local_mode.md"])
    assert page == File.read!("docs/topics/basics/local_mode.md")
    assert {:text, ^page} = Knowledge.call("Flezha.docs", ["local_mode"])
  end

  test "an unknown value is refused with a hint that names the way on" do
    assert {:error, %{code: "mcp.unknown_language", hint: hint}} =
             Knowledge.call("Flezha.sdk_example", ["cobol"])

    assert hint =~ "elixir, java, node, php, python, ruby, rust"

    for page <- ["no_such_page", "../rules.md", "../../../mix.exs", nil] do
      assert {:error, %{code: "mcp.unknown_page", hint: hint}} =
               Knowledge.call("Flezha.docs", [page])

      assert hint =~ "flezha_guide"
    end

    assert {:error, %{code: "mcp.unknown_code"}} =
             Knowledge.call("Flezha.error", ["no.such_code"])
  end

  test "a new project holds every file of the SDK's starter, its .gitignore included" do
    assert {:ok, json} = Knowledge.call("Flezha.new_project", ["python", nil])
    assert %{"files" => files, "run" => run} = Jason.decode!(json)

    {listed, 0} = System.cmd("git", ~w(-C sdk/python ls-files examples/starter))

    paths =
      for file <- String.split(listed, "\n", trim: true),
          do: Path.relative_to(file, "examples/starter")

    assert Enum.sort(Enum.map(files, & &1["path"])) == Enum.sort(paths)

    for %{"path" => path, "content" => content, "executable" => false} <- files,
        do: assert(content == File.read!(Path.join("sdk/python/examples/starter", path)))

    assert ".venv/bin/python runner.py" in run
  end

  test "an unknown language or example is refused naming flz template list" do
    for args <- [["cobol", nil], ["java", "nope"]] do
      assert {:error, %{code: "template.unknown", hint: "flz template list"}} =
               Knowledge.call("Flezha.new_project", args)
    end
  end

  # The rules name tools, `flz` and what an error carries, never an SDK's own
  # functions or classes, so nothing in them goes stale when an SDK changes.
  test "every code span of the rules is a tool, flz, an error field or code, or a page" do
    {:text, guide} = Knowledge.call("Flezha.guide", [])
    pages = for [_, page] <- Regex.scan(~r/\]\(([^)]+\.md)\)/, guide), do: page
    tools = Enum.map(Knowledge.entries(), &Mcp.tool_name(&1.name))
    operations = Enum.map(Flezha.Service.catalogue(), &Mcp.tool_name(&1.name))
    codes = Map.keys(Flezha.Error.catalogue()["codes"])
    allowed = tools ++ operations ++ codes ++ pages ++ ~w(flz --json code hint)

    for [_, span] <- Regex.scan(~r/`([^`]+)`/, Knowledge.rules()),
        do: assert(span in allowed, "the rules name `#{span}`")
  end
end
