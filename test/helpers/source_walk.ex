defmodule FlezhaTest.Helper.SourceWalk do
  @moduledoc """
  The tree as text, for a gate that judges the source it walks.

  A seam test asks the same three questions of the tree — which files are
  source, what do they say, and where does each line sit — and the answers have
  to be the same for every one of them. Two rules hold:

    * **Source is a source extension.** A `.pyc` beside a `.py` and a `.so`
      beside a `.rs` carry every string their source does, so a walk that reads
      them reports names nobody wrote.
    * **A file the tree holds is walked, tracked or not.** A blind walk always
      flatters; an untracked module is still a module, and a bypass written into
      one is still a bypass.
  """

  @source_extensions ~w(.ex .exs .rs .java .ts .mjs .js .py .rb .php .toml .sh .eex)

  @doc "Every source file under `roots`, in a stable order."
  @spec files([binary()]) :: [binary()]
  def files(roots) do
    for root <- roots,
        path <- Path.wildcard("#{root}/**/*"),
        Path.extname(path) in @source_extensions,
        File.regular?(path),
        uniq: true,
        do: path
  end

  @doc "Every source file under `roots` whose extension is one of `extensions`."
  @spec files([binary()], [binary()]) :: [binary()]
  def files(roots, extensions),
    do: for(path <- files(roots), Path.extname(path) in extensions, do: path)

  @doc "The contents of `paths`, joined — for a gate that asks whether a name appears."
  @spec text([binary()]) :: binary()
  def text(paths), do: Enum.map_join(paths, "\n", &File.read!/1)

  @doc "Every line of `paths` as `{path, line, number}`, numbered from one."
  @spec lines([binary()]) :: [{binary(), binary(), pos_integer()}]
  def lines(paths) do
    for path <- paths,
        {line, number} <- Enum.with_index(String.split(File.read!(path), "\n"), 1),
        do: {path, line, number}
  end
end
