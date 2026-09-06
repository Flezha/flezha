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

  Every answer is held for the run. The tree cannot change while a suite is in
  flight, and a gate asks the same question once per clause: `Flezha.Process.SeamTest`
  alone walked `lib/flezha` six times, once per rule it judges.
  """

  @source_extensions ~w(.ex .exs .rs .java .ts .mjs .js .py .rb .php .toml .sh .eex)

  @doc "Every source file under `roots`, in a stable order."
  @spec files([binary()]) :: [binary()]
  def files(roots) do
    held({:files, roots}, fn ->
      for root <- roots,
          path <- Path.wildcard("#{root}/**/*"),
          Path.extname(path) in @source_extensions,
          File.regular?(path),
          uniq: true,
          do: path
    end)
  end

  @doc "Every source file under `roots` whose extension is one of `extensions`."
  @spec files([binary()], [binary()]) :: [binary()]
  def files(roots, extensions),
    do: for(path <- files(roots), Path.extname(path) in extensions, do: path)

  @doc "The contents of `paths`, joined — for a gate that asks whether a name appears."
  @spec text([binary()]) :: binary()
  def text(paths), do: Enum.map_join(paths, "\n", &read/1)

  @doc "Every line of `paths` as `{path, line, number}`, numbered from one."
  @spec lines([binary()]) :: [{binary(), binary(), pos_integer()}]
  def lines(paths), do: Enum.flat_map(paths, &held({:lines, &1}, fn -> number(&1) end))

  @doc """
  `lines/1` without the lines that are only a comment.

  A comment that names a primitive does not use it, so every gate judging a
  shape drops them, and dropping them per clause is a second regex over every
  line of the tree. `scripts/check-layer-direction.sh` strips prose the same way
  for the same reason.
  """
  @spec code_lines([binary()]) :: [{binary(), binary(), pos_integer()}]
  def code_lines(paths) do
    Enum.flat_map(paths, fn path ->
      held({:code_lines, path}, fn ->
        Enum.reject(number(path), fn {_p, line, _n} -> Regex.match?(~r/^\s*#/, line) end)
      end)
    end)
  end

  @doc "The contents of `path`, read once for the run."
  @spec read(binary()) :: binary()
  def read(path), do: held({:read, path}, fn -> File.read!(path) end)

  @doc """
  `path`'s quoted form, parsed once for the run.

  A gate that judges a shape rather than a spelling reads the AST, and parsing
  is what such a gate spends its time on: `Flezha.Convergence.VerdictSeamTest`
  parses every file of `lib/flezha` in each of two walks.
  """
  @spec ast(binary()) :: Macro.t()
  def ast(path) do
    held({:ast, path}, fn ->
      {:ok, quoted} = Code.string_to_quoted(read(path))
      quoted
    end)
  end

  defp number(path) do
    for {line, number} <- Enum.with_index(String.split(read(path), "\n"), 1),
        do: {path, line, number}
  end

  # Two modules racing for the same key both build it and both store the same
  # value, which is why this needs no lock.
  defp held(key, build) do
    case :persistent_term.get({__MODULE__, key}, :absent) do
      :absent ->
        value = build.()
        :persistent_term.put({__MODULE__, key}, value)
        value

      value ->
        value
    end
  end
end
