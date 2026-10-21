defmodule Mix.Tasks.Flezha.AiKit do
  @shortdoc "Write priv/ai_kit: what flz mcp's knowledge tools serve"

  @moduledoc """
  Writes the files `Flezha.Mcp.Knowledge` reads into `priv/ai_kit/`, so a
  release answers an assistant without the source tree:

  - `sdk/<language>.md`: every tracked file of that SDK's `examples/echo`, the
    code Up and running runs, each under its path;
  - `templates/<language>/<example>/`: a copy of every tracked file of that
    SDK's `examples/<example>` but `.gitignore`, and `templates/manifest.json`:
    each template's files with their executable bit, its `.gitignore` text,
    the summary its README's title gives and the commands of its "Run it"
    section, which `flz template` and `flezha_new_project` serve;
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

  alias Flezha.Mcp.Knowledge

  @root "priv/ai_kit"

  # Each a directory of `examples/` in every SDK.
  @examples ~w(starter)

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
    doc_dir = List.first(args, "_build/.doc")
    map = map(File.read!(Path.join(doc_dir, "llms.txt")))

    for dir <- ["sdk", "docs", "templates"], do: File.rm_rf!(Path.join(@root, dir))

    for {path, body} <- files(map) do
      full = Path.join(@root, path)
      File.mkdir_p!(Path.dirname(full))
      File.write!(full, body)
    end

    Mix.shell().info("wrote #{@root}")
  end

  @doc """
  The files under `root` that differ from what `run/1` would write with the
  map `root` holds: an SDK example, a guide page or a template file, one that
  is missing, or one that is no longer written.
  """
  @spec stale(Path.t()) :: [Path.t()]
  def stale(root \\ @root) do
    expected = files(committed(root, "llms.txt") || "")
    written = Map.new(expected, fn {path, _body} -> {path, true} end)

    extra =
      for dir <- ["docs", "templates"],
          full <- Path.wildcard(Path.join([root, dir, "**"]), match_dot: true),
          File.regular?(full),
          path = Path.relative_to(full, root),
          not Map.has_key?(written, path),
          do: path

    differing = for {path, body} <- expected, committed(root, path) != body, do: path
    Enum.sort(differing ++ extra)
  end

  # Everything the kit holds but the hand-written rules, for the map `map`.
  defp files(map) do
    [{"llms.txt", map}] ++
      sdk_files() ++
      template_files() ++
      for page <- linked(map), do: {"docs/" <> page, File.read!(source!(page))}
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
    for language <- Knowledge.languages(),
        do: {"sdk/#{language}.md", example(language)}
  end

  defp example(language) do
    files = Enum.reject(listed(language, "echo"), &(Path.basename(&1) == ".gitignore"))

    IO.iodata_to_binary([
      "# The #{language} SDK's echo example\n\n",
      "Every file of `sdk/#{language}/examples/echo`, the runner Up and running builds. ",
      "Its dependency file names the SDK package and the version this release pins.\n",
      for file <- files do
        fence = Map.get(@fences, Path.extname(file), "")
        body = File.read!("sdk/#{language}/#{file}")
        ["\n## ", file, "\n\n````", fence, "\n", body, newline(body), "````\n"]
      end
    ])
  end

  defp template_files do
    {entries, copies} =
      Enum.unzip(
        for language <- Knowledge.languages(),
            example <- @examples,
            do: template(language, example)
      )

    manifest = Jason.encode!(%{templates: entries}, pretty: true)
    [{"templates/manifest.json", manifest <> "\n"} | Enum.concat(copies)]
  end

  # One template's manifest entry, and its copies.
  defp template(language, example) do
    from = "sdk/#{language}/examples/#{example}/"

    {ignores, files} =
      language
      |> listed(example)
      |> Enum.map(&String.replace_prefix(&1, "examples/#{example}/", ""))
      |> Enum.split_with(&(Path.basename(&1) == ".gitignore"))

    readme = File.read!(from <> "README.md")

    entry = %{
      language: language,
      example: example,
      summary: summary(readme, from),
      run: run_commands(readme, from),
      gitignore: if(".gitignore" in ignores, do: File.read!(from <> ".gitignore")),
      files: for(file <- files, do: %{path: file, executable: executable?(from <> file)})
    }

    {entry,
     for(file <- files, do: {"templates/#{language}/#{example}/#{file}", text!(from <> file)})}
  end

  @doc """
  A template's one-line summary: its README's title after the first colon, so
  "Starter: a relay that ..." is "A relay that ...". A title with no colon
  refuses the generation, naming the README under `from`.
  """
  @spec summary(String.t(), Path.t()) :: String.t()
  def summary(readme, from) do
    [title | _] = String.split(readme, "\n", parts: 2)

    case String.split(String.trim_leading(title, "# "), ": ", parts: 2) do
      [_, words] -> String.upcase(String.first(words)) <> String.slice(words, 1..-1//1)
      [_] -> Mix.raise("#{from}README.md's title has no \": \" before its summary")
    end
  end

  # Every line of the `sh` blocks of the README's "Run it" section.
  defp run_commands(readme, from) do
    section =
      case Regex.run(~r/^## Run it\n(.*?)(?=^## |\z)/ms, readme) do
        [_, section] -> section
        nil -> Mix.raise("#{from}README.md has no \"## Run it\" section")
      end

    for [_, block] <- Regex.scan(~r/^```sh\n(.*?)^```/ms, section),
        line <- String.split(block, "\n", trim: true),
        do: line
  end

  defp executable?(path), do: Bitwise.band(File.stat!(path).mode, 0o111) != 0

  # The tools answer JSON, which carries text only.
  defp text!(path) do
    body = File.read!(path)
    if String.valid?(body), do: body, else: Mix.raise("#{path} is not UTF-8 text")
  end

  # What git tracks under an SDK's `examples/<example>`: never a stray working
  # file, whoever's checkout it is.
  defp listed(language, example) do
    args = ["-C", "sdk/#{language}", "ls-files", "examples/#{example}"]

    case System.cmd("git", args, stderr_to_stdout: true) do
      {listed, 0} when listed != "" -> listed |> String.split("\n", trim: true) |> Enum.sort()
      _none -> Mix.raise("sdk/#{language} lists no examples/#{example}: check out its submodule")
    end
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
