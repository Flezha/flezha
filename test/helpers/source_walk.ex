defmodule FlezhaTest.Helper.SourceWalk do
  @moduledoc """
  The tree as text, for a gate that judges the source it walks.

  A seam test asks the same three questions of the tree — which files are
  source, what do they say, and where does each line sit — and the answers have
  to be the same for every one of them. Three rules hold:

    * **Source is a source extension.** A `.pyc` beside a `.py` and a `.so`
      beside a `.rs` carry every string their source does, so a walk that reads
      them reports names nobody wrote.
    * **A file the tree holds is walked, tracked or not.** A blind walk always
      flatters; an untracked module is still a module, and a bypass written into
      one is still a bypass.
    * **Build output is never entered.** What `_build`, `deps`, `target`,
      `dist`, `cover`, `node_modules` and `vendor` hold was built or fetched, not written
      here, and a runner's `node_modules` links back to its own SDK: a walk that
      follows it reads that SDK again at every turn of the loop.

  Every answer is held for the run. The tree cannot change while a suite is in
  flight, and a gate asks the same question once per clause: `Flezha.Process.SeamTest`
  alone walked `lib/flezha` six times, once per rule it judges.
  """

  @source_extensions ~w(.ex .exs .rs .java .ts .mjs .js .py .rb .php .toml .sh .eex)

  @build_output ~w(node_modules vendor target dist _build deps cover)

  @doc """
  Every file under `roots`, whatever its extension, in a stable order: for a
  gate that reads documents and steps relays as well as code. A root that is a
  file is itself.
  """
  @spec tree([binary()]) :: [binary()]
  def tree(roots) do
    held({:tree, roots}, fn ->
      for root <- roots, path <- Enum.sort(walk(root)), uniq: true, do: path
    end)
  end

  @doc "Every source file under `roots`, in a stable order."
  @spec files([binary()]) :: [binary()]
  def files(roots), do: files(roots, @source_extensions)

  @doc "Every file under `roots` whose extension is one of `extensions`."
  @spec files([binary()], [binary()]) :: [binary()]
  def files(roots, extensions),
    do: for(path <- tree(roots), Path.extname(path) in extensions, do: path)

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
    # Bound once: a regex literal is rebuilt every time it is evaluated.
    comment = ~r/^\s*#/
    Enum.flat_map(paths, fn path -> held({:code_lines, path}, fn -> code(path, comment) end) end)
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

  defp code(path, comment),
    do: Enum.reject(number(path), fn {_p, line, _n} -> Regex.match?(comment, line) end)

  # A dot entry is tool state (`.git`, `.venv`), never source a gate judges.
  defp walk(path) do
    case File.ls(path) do
      {:ok, names} ->
        for name <- names,
            not String.starts_with?(name, "."),
            name not in @build_output,
            file <- walk(Path.join(path, name)),
            do: file

      {:error, _not_a_directory} ->
        if File.regular?(path), do: [path], else: []
    end
  end

  # Two modules racing for the same key both build it and both store the same
  # value, which is why this needs no lock. Not `:persistent_term`: a new key
  # there waits for every scheduler to make progress, which a loaded suite
  # turns into milliseconds per file.
  defp held(key, build) do
    table = table()

    case :ets.lookup(table, key) do
      [{^key, value}] ->
        value

      [] ->
        value = build.()
        :ets.insert(table, {key, value})
        value
    end
  end

  # An unlinked agent owns the table, so it outlives the test that created it.
  defp table do
    with :undefined <- :ets.whereis(__MODULE__) do
      create = fn -> :ets.new(__MODULE__, [:named_table, :public, read_concurrency: true]) end

      case Agent.start(create, name: __MODULE__) do
        {:ok, _owner} -> __MODULE__
        {:error, {:already_started, owner}} -> Agent.get(owner, & &1)
      end
    end
  end
end
