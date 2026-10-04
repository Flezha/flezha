defmodule Mix.Tasks.Flezha.AiKit do
  @shortdoc "Write priv/ai_kit: what flz mcp's knowledge tools serve"

  @moduledoc """
  Writes the files `Flezha.Mcp.Knowledge` reads into `priv/ai_kit/`, so a
  release answers an assistant without the source tree:

  - `sdk/<language>.md`: every tracked file of that SDK's `examples/echo`, the
    code the tutorials run, each under its path;
  - `llms.txt`: the assistant map `mix flezha.llms` wrote into `doc_dir`, less
    the published API pages and the catalogue, which the operation tools carry;
  - `docs/<page>.md`: each guide page that map links, copied from its source.

  `rules.md` is written by hand. The `docs` alias runs this task after the map.
  `--check` regenerates in memory and fails naming every file that differs,
  which the suite does too; it reads no `doc_dir`, so the map is held only to
  the pages it links.

      mix docs
      mix flezha.ai_kit [doc_dir]
      mix flezha.ai_kit --check
  """

  use Mix.Task

  @root "priv/ai_kit"

  @fences %{
    ".ex" => "elixir",
    ".exs" => "elixir",
    ".java" => "java",
    ".mjs" => "javascript",
    ".json" => "json",
    ".php" => "php",
    ".py" => "python",
    ".rb" => "ruby",
    ".rs" => "rust",
    ".toml" => "toml",
    ".xml" => "xml",
    ".md" => "markdown"
  }

  @impl Mix.Task
  def run(["--check"]) do
    case stale() do
      [] -> Mix.shell().info("#{@root} is fresh")
      paths -> Mix.raise("#{@root} is stale: #{Enum.join(paths, ", ")}; run `mix docs`")
    end
  end

  def run(args) do
    doc_dir = List.first(args, "doc")
    map = map(File.read!(Path.join(doc_dir, "llms.txt")))

    for dir <- ["sdk", "docs"], do: File.rm_rf!(Path.join(@root, dir))

    for {path, body} <- files(map) do
      full = Path.join(@root, path)
      File.mkdir_p!(Path.dirname(full))
      File.write!(full, body)
    end

    Mix.shell().info("wrote #{@root}")
  end

  @doc """
  The files under `root` that differ from what `run/1` would write with the
  map `root` holds: an SDK example, a guide page, or a page that is missing or
  that the map no longer links.
  """
  @spec stale(Path.t()) :: [Path.t()]
  def stale(root \\ @root) do
    expected = files(committed(root, "llms.txt") || "")
    written = Map.new(expected, fn {path, _body} -> {path, true} end)

    extra =
      case File.ls(Path.join(root, "docs")) do
        {:ok, pages} ->
          for page <- pages, not Map.has_key?(written, "docs/" <> page), do: "docs/" <> page

        {:error, _} ->
          []
      end

    differing = for {path, body} <- expected, committed(root, path) != body, do: path
    Enum.sort(differing ++ extra)
  end

  # Everything the kit holds but the hand-written rules, for the map `map`.
  defp files(map) do
    [{"llms.txt", map}] ++
      sdk_files() ++ for page <- linked(map), do: {"docs/" <> page, File.read!(source!(page))}
  end

  @doc "`doc_dir`'s assistant map less the sections the knowledge tools do not serve."
  @spec map(String.t()) :: String.t()
  def map(llms) do
    llms
    |> String.replace(~r/^- \[[^\]]*\]\(catalogue\.json\).*\n/m, "")
    |> String.replace(~r/\n## Published API\n.*\z/s, "\n")
  end

  defp linked(map) do
    for [_, page] <- Regex.scan(~r/\]\(([^)\/]+\.md)\)/, map), uniq: true, do: page
  end

  defp sdk_files do
    for language <- Flezha.Mcp.Knowledge.languages(),
        do: {"sdk/#{language}.md", example(language)}
  end

  defp example(language) do
    listed =
      case System.cmd("git", ["-C", "sdk/#{language}", "ls-files", "examples/echo"],
             stderr_to_stdout: true
           ) do
        {listed, 0} when listed != "" -> listed
        _none -> Mix.raise("sdk/#{language} lists no examples/echo: check out its submodule")
      end

    files =
      listed
      |> String.split("\n", trim: true)
      |> Enum.reject(&(Path.basename(&1) == ".gitignore"))
      |> Enum.sort()

    IO.iodata_to_binary([
      "# The #{language} SDK's echo example\n\n",
      "Every file of `sdk/#{language}/examples/echo`, the runner the echo tutorial runs. ",
      "Its dependency file names the SDK package and the version this release pins.\n",
      for file <- files do
        fence = Map.get(@fences, Path.extname(file), "")
        body = File.read!("sdk/#{language}/#{file}")
        ["\n## ", file, "\n\n````", fence, "\n", body, newline(body), "````\n"]
      end
    ])
  end

  defp newline(body), do: if(String.ends_with?(body, "\n"), do: "", else: "\n")

  defp committed(root, path) do
    case File.read(Path.join(root, path)) do
      {:ok, body} -> body
      {:error, _} -> nil
    end
  end

  defp source!(page) do
    extras = for extra <- Mix.Project.config()[:docs][:extras], do: extra_path(extra)

    case Enum.find(extras, &(Path.basename(&1) == page)) do
      nil -> Mix.raise("the map links #{page}, which no extras page of mix.exs is")
      path -> path
    end
  end

  defp extra_path({path, _opts}), do: to_string(path)
  defp extra_path(path), do: to_string(path)
end
