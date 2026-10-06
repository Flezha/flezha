defmodule Mix.Tasks.Flezha.Llms do
  @shortdoc "Rewrite doc/llms.txt as a small map for AI assistants"

  @moduledoc """
  Rewrites the `doc/llms.txt` ExDoc writes into a map an assistant reads whole:
  a "Start here" list, every user-facing guide page with the first sentence of
  its introduction, and the published API modules. Each line links the
  Markdown copy ExDoc writes next to it, so an assistant opens only the pages a
  task needs. Also copies `cli/catalogue.json` next to it.

  The `docs` alias runs it on the folder ExDoc wrote its Markdown to:

      mix docs
      mix flezha.llms [doc_dir]
  """

  use Mix.Task

  alias Flezha.AuthZ.Verb
  alias Mix.Tasks.Flezha.Catalogue

  # Pages for people changing Flezha, not using it, and the links into the
  # tutorial site, which ExDoc lists as pages but writes no copy of.
  @excluded_groups ["Architecture", "Contributing", "Tutorials"]
  @start_here ["what_is_flezha.md", "local_mode.md", "cli.md", "errors.md"]
  @max_bytes 32 * 1024

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("compile")
    doc_dir = List.first(args, "doc")
    map = render(doc_dir)
    File.write!(Path.join(doc_dir, "llms.txt"), map)
    File.cp!(Catalogue.path(), Path.join(doc_dir, "catalogue.json"))
    Mix.shell().info("wrote #{Path.join(doc_dir, "llms.txt")} (#{byte_size(map)} bytes)")
  end

  @doc """
  The map, from ExDoc's `llms.txt` and the page and module copies in `doc_dir`.
  """
  @spec render(Path.t()) :: String.t()
  def render(doc_dir) do
    exdoc = File.read!(Path.join(doc_dir, "llms.txt"))
    groups = Enum.reject(exdoc_groups(doc_dir, exdoc), &(elem(&1, 0) in @excluded_groups))
    titles = Map.new(for {_, pages} <- groups, {title, file} <- pages, do: {file, title})

    api =
      for module <- Flezha.Service.modules() ++ [Flezha.Error],
          do: {inspect(module), "#{inspect(module)}.md"}

    map =
      IO.iodata_to_binary([
        "# Flezha\n\n",
        Regex.run(~r/^> .*\n/m, exdoc) || [],
        "\nThis map lists every guide page and the published API. Read it whole, then open\n",
        "only the pages a task needs: each link is that page's full Markdown, next to this file.\n",
        "\n## Start here\n\n",
        Enum.map(@start_here, &"- [#{start_title(doc_dir, titles, &1)}](#{&1})\n"),
        "- [Operation catalogue](catalogue.json): every published operation and its arguments, ",
        "as JSON\n",
        Enum.map(groups ++ [{"Published API", api}], fn {group, pages} ->
          ["\n## #{group}\n\n", Enum.map(pages, &line(doc_dir, &1))]
        end)
      ])

    if byte_size(map) > @max_bytes do
      Mix.raise(
        "#{doc_dir}/llms.txt would be #{byte_size(map)} bytes, over the #{@max_bytes} limit"
      )
    end

    map
  end

  # ExDoc's "## Pages" section: `- Group` lines, each followed by its
  # `  - [Title](file.md)` lines, in sidebar order.
  defp exdoc_groups(doc_dir, exdoc) do
    case Regex.run(~r/^## Pages\n(.*?)(?=^## |\z)/ms, exdoc) do
      [_, pages] ->
        for chunk <- String.split(String.trim(pages), ~r/^- /m, trim: true),
            do: group(doc_dir, String.split(chunk, "\n", trim: true))

      nil ->
        Mix.raise("#{doc_dir}/llms.txt has no ExDoc page list; run `mix docs` to write it again")
    end
  end

  defp group(doc_dir, ["[" <> _ = page | _]) do
    Mix.raise("#{doc_dir}/llms.txt lists #{page} in no group; add it to groups_for_extras")
  end

  defp group(_doc_dir, [group | lines]), do: {group, Enum.map(lines, &page/1)}

  defp page(line) do
    [_, title, file] = Regex.run(~r/^\s+- \[(.+)\]\((.+)\)$/, line)
    {title, file}
  end

  defp start_title(doc_dir, titles, file) do
    Map.get_lazy(titles, file, fn ->
      Mix.raise("#{doc_dir}/llms.txt lists no #{file} for the start list")
    end)
  end

  defp line(doc_dir, {title, file}) do
    markdown =
      case File.read(Path.join(doc_dir, file)) do
        {:ok, markdown} ->
          markdown

        {:error, _} ->
          Mix.raise("#{doc_dir}/#{file} is missing: ExDoc wrote no Markdown copy of it")
      end

    case first_sentence(markdown) do
      nil -> "- [#{title}](#{file})\n"
      sentence -> "- [#{title}](#{file}): #{sentence}\n"
    end
  end

  # The first sentence of the first prose paragraph before the first section:
  # comments, code fences, headings, lists, tables and the contents block are
  # not prose.
  defp first_sentence(markdown) do
    markdown
    |> String.replace(~r/<!--.*?-->/s, "")
    |> String.replace(~r/^```.*?^```/ms, "")
    |> String.replace(~r/^## .*/ms, "")
    |> String.split(~r/\n\s*\n/)
    |> Enum.map(&String.trim/1)
    |> Enum.find(&(&1 =~ ~r/^[\p{L}`\[]/u))
    |> Verb.first_sentence()
  end
end
