defmodule Flezha.Config.KeysSeamTest do
  @moduledoc """
  A knob is one row of one table, and a bypass is impossible.

  Five clauses: nothing reads a knob outside the seam; every row is complete
  and every name in the tree is a row; a default is written down once; every
  runtime that reads a name is declared to; and every runtime's own suite is
  held to the one parse rule.
  """
  use ExUnit.Case, async: true

  alias Flezha.Config.Keys
  alias Flezha.Config.Reference
  alias FlezhaTest.Helper.SourceWalk

  # The readers. Everything else asks one of these.
  @seam [
    "lib/flezha/config.ex",
    "lib/flezha/config/keys.ex",
    "lib/flezha/config/durations.ex",
    "lib/flezha/config/environment.ex",
    "flz_transport/src/env.rs"
  ]

  # A read of a NAMED knob: a literal `FLEZHA_*` or a module attribute holding
  # one. `System.get_env(value)` where the value is data a user wrote into
  # a setting — a password variable's name — is not a knob and no row can
  # declare it, so the shape lets a lowercase argument through.
  @elixir_read ~r/System\.get_env\("FLEZHA_|System\.get_env\(@|Application\.(get_env|fetch_env!?)\(:flezha/
  @rust_read ~r/env::var(_os)?\(("FLEZHA_|[A-Z][A-Z0-9_]*\))/

  # A regex attribute or literal is re-imported every time it is evaluated, so
  # a clause that runs one over every line of the tree binds it once first.
  @env_name ~r/FLEZHA_[A-Z0-9_]+/

  @rust_roots ~w(flz_core/src flz_transport/src flz_nif/src flz_core_ffi/src sdk/rust/src)

  # Where each runtime's source lives, for the declaration clause.
  @runtime_roots %{
    elixir_sdk: ["sdk/elixir/lib"],
    rust: @rust_roots,
    java: ["sdk/java/flezha-sdk-runtime/src/main"],
    node: ["sdk/node/src"],
    python: ["sdk/python/src"],
    ruby: ["sdk/ruby/lib"],
    php: ["sdk/php/src"]
  }

  # The published reference: each page and the groups of rows it carries.
  @reference %{
    "docs/public/configuration/environment_variables.md" => [:environment, :build],
    "docs/public/configuration/application_env.md" => [:application_env],
    "docs/public/configuration/runner_environment.md" => [:runner]
  }

  # The fixture every runtime's own reader is fed, and the suite that feeds it.
  @parse_fixture "test/fixtures/knob_parse_cases.txt"
  @parse_readers [
    "test/flezha/config/parse_test.exs",
    "sdk/elixir/test/flezha/sdk/knob_test.exs",
    "flz_transport/src/env.rs",
    "sdk/java/flezha-sdk-runtime/src/test/java/com/flezha/sdk/runtime/config/KnobParseRuleTest.java",
    "sdk/node/test/env.test.mjs",
    "sdk/python/tests/test_env.py",
    "sdk/ruby/test/env_test.rb",
    "sdk/php/tests/EnvTest.php"
  ]

  describe "nothing reads a knob outside the seam" do
    test "an application-env or environment read names the file, the line and its row" do
      elixir_read = @elixir_read

      offences =
        for {path, line, number} <- lines(elixir_sources()),
            path not in @seam,
            Regex.match?(elixir_read, line),
            do: offence(path, number, line)

      assert offences == []
    end

    test "a crate that reads the environment itself names the file, the line and its row" do
      rust_read = @rust_read

      offences =
        for {path, line, number} <- lines(rust_sources()),
            path not in @seam,
            Regex.match?(rust_read, line),
            do: offence(path, number, line)

      assert offences == []
    end
  end

  describe "every row is complete, and every name is a row" do
    test "a row states a type and at least one runtime that reads it" do
      incomplete =
        for {name, row} <- Keys.rows(),
            row.runtimes == [] or is_nil(row.type),
            do: name

      assert incomplete == []
    end

    test "a derivation names a row that exists, and carries no default of its own" do
      broken =
        for {name, row} <- Keys.rows(),
            row.derived != nil,
            parent = elem(row.derived, 1),
            is_nil(Keys.row(parent)) or row.default != nil,
            do: "#{name} derives from #{parent}"

      assert broken == []
    end

    test "every application key a config file writes is a row" do
      declared =
        for {_name, row} <- Keys.rows(), row.key, into: MapSet.new(), do: root_key(row.key)

      written =
        for path <- ["config/config.exs", "config/runtime.exs"],
            {key, _value} <- Config.Reader.read!(path, env: :test, target: :host)[:flezha] || [],
            into: MapSet.new(),
            do: key

      assert MapSet.difference(written, declared) |> MapSet.to_list() == []
    end

    test "every variable named in the controller, its crates and its SDKs is a row" do
      env_name = @env_name

      undeclared =
        for {path, line, number} <- lines(elixir_sources() ++ rust_sources() ++ sdk_sources()),
            [var] <- Regex.scan(env_name, line),
            is_nil(Keys.by_env(var)),
            uniq: true,
            do: "#{path}:#{number}: #{var} is named and no row declares it"

      assert undeclared == []
    end
  end

  describe "a row nothing reads does not exist" do
    test "every controller row is named by a site, by its group, or by a derivation" do
      sites = SourceWalk.text(elixir_sources() -- @seam)

      groups =
        for [_, prefix] <- Regex.scan(~r/Config\.group\(([^)]+)\)/, sites),
            into: MapSet.new(),
            do: prefix |> String.trim_leading("{") |> String.split(",") |> hd() |> String.trim()

      parents =
        for {_name, row} <- Keys.rows(), row.derived, into: MapSet.new(), do: elem(row.derived, 1)

      unread =
        for {name, row} <- Keys.rows(),
            :controller in row.runtimes,
            not String.contains?(sites, ":#{name}"),
            not MapSet.member?(parents, name),
            not grouped?(row.key, groups),
            do: name

      assert unread == []
    end
  end

  describe "a default is written down once" do
    test "each reference table is its group's rendering of the rows" do
      for {page, groups} <- @reference, group <- groups do
        rendered = Reference.render_table(group)

        assert SourceWalk.block(page, "config-keys", to_string(group)) == rendered,
               "#{page}'s #{group} table has drifted from the rows.\n" <>
                 "Replace the block between its config-keys markers with:\n\n" <> rendered
      end
    end

    test "no other page restates a variable's default in a table of its own" do
      restated =
        for path <- Path.wildcard("docs/**/*.md"),
            not Map.has_key?(@reference, path),
            not String.starts_with?(path, ["docs/refactor/", "docs/dev_notes/"]),
            where <- restated_rows(path),
            do: where

      assert restated == [],
             "link the reference instead of restating its rows:\n" <> Enum.join(restated, "\n")
    end

    test "a crate's own default beside a name is the row's" do
      # Every `DEFAULT_*` constant a crate hands its env reader for a name this
      # table declares. Two independent statements of one value, so a change to
      # either is a red build rather than a silent divergence.
      handed = ~r/env_\w+\(\s*"(FLEZHA_[A-Z0-9_]+)",\s*([A-Z][A-Z0-9_]*)/

      drifted =
        for {path, line, number} <- lines(rust_sources()),
            [_, var, constant] <- Regex.scan(handed, line),
            row = Keys.by_env(var),
            row != nil,
            row.default != nil,
            declared = rust_const(constant),
            declared != nil,
            to_string(row.default) != to_string(declared),
            do:
              "#{path}:#{number}: #{var} is #{declared} in the crate and #{row.default} in the row"

      assert drifted == []
    end

    test "the Elixir SDK's own rows are the table's" do
      # The SDK is a package of its own and declares what it reads, as every
      # runtime's reader does. Its variable, type and runner-side default are
      # the row's, so a change to either is a red build.
      drifted =
        for {name, sdk} <- Flezha.SDK.Knob.rows(),
            row = Keys.row(name),
            declared = row && {row.env, row.type, Flezha.Config.default(name, :runner)},
            declared != {sdk.env, sdk.type, sdk.default} or :elixir_sdk not in row.runtimes,
            do:
              "#{name}: the SDK reads #{inspect({sdk.env, sdk.type, sdk.default})}, the table #{inspect(declared)}"

      undeclared = for {name, _} <- Flezha.SDK.Knob.rows(), is_nil(Keys.row(name)), do: name

      assert {undeclared, drifted} == {[], []}
    end
  end

  describe "every runtime that reads a name is declared to" do
    test "a name read in an SDK's own source is a row that names that SDK" do
      env_name = @env_name

      undeclared =
        for {runtime, roots} <- @runtime_roots,
            {path, line, _number} <- lines(sources(roots)),
            [var] <- Regex.scan(env_name, line),
            row = Keys.by_env(var),
            row != nil,
            runtime not in row.runtimes,
            uniq: true,
            do: "#{path}: #{var} is read by #{runtime}, which its row does not name"

      assert undeclared == []
    end
  end

  describe "the parse rule holds per runtime" do
    test "every runtime's own suite feeds the shared fixture to its own reader" do
      unfed =
        for path <- @parse_readers,
            not String.contains?(File.read!(path), Path.basename(@parse_fixture)),
            do: path

      assert unfed == []
    end
  end

  # ---------- the tree, as text ----------

  defp elixir_sources(), do: SourceWalk.files(["lib", "config"], [".ex", ".exs"])

  defp rust_sources(), do: sources(@rust_roots)

  defp sdk_sources(), do: @runtime_roots |> Map.values() |> List.flatten() |> sources()

  defp sources(roots), do: SourceWalk.files(roots)

  defp lines(paths), do: paths |> Enum.uniq() |> SourceWalk.lines()

  # The remedy, not just the offence: the row this line should have read, found
  # by the variable it names or by the application key it reaches for.
  defp offence(path, number, line) do
    "#{path}:#{number}: #{String.trim(line)} — " <>
      case row_for(line) do
        nil -> "no row declares this"
        row -> "read Flezha.Config.get(:#{row.name})"
      end
  end

  defp row_for(line), do: row_by_variable(line) || row_by_key(line)

  defp row_by_variable(line) do
    case Regex.run(@env_name, line) do
      [var] -> Keys.by_env(var)
      nil -> nil
    end
  end

  defp row_by_key(line) do
    case Regex.run(~r/\(:flezha, :([a-z_0-9]+)/, line) do
      [_, key] -> row_for_key(String.to_atom(key))
      nil -> nil
    end
  end

  defp row_for_key(key),
    do: Enum.find_value(Keys.rows(), fn {_name, row} -> row.key == key && row end)

  # The rows of a table with a `Default` column that name a variable.
  defp restated_rows(path) do
    [path]
    |> SourceWalk.lines()
    |> Enum.reduce({false, []}, fn {_path, line, number}, {defaults?, found} ->
      cond do
        not String.starts_with?(line, "|") -> {false, found}
        String.contains?(line, "| Default |") -> {true, found}
        defaults? and line =~ ~r/^\| `FLEZHA_/ -> {true, ["#{path}:#{number}" | found]}
        true -> {defaults?, found}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  # `const NAME: usize = 64 * 1024 * 1024;` — the arithmetic a crate writes its
  # sizes as, evaluated the way the crate does.
  defp rust_const(name) do
    with [path | _] <-
           Enum.filter(rust_sources(), &String.contains?(SourceWalk.read(&1), "#{name}:")),
         [[_, expression]] <-
           Regex.scan(~r/#{name}: [a-z0-9]+ = ([0-9_ *+]+);/, SourceWalk.read(path)) do
      expression
      |> String.replace("_", "")
      |> String.split("*")
      |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
      |> Enum.product()
    else
      _ -> nil
    end
  end

  # A subsystem that takes its options as a group reads every row under it.
  defp grouped?(key, groups) when is_tuple(key) do
    MapSet.member?(groups, ":#{elem(key, 0)}") or
      (tuple_size(key) > 2 and MapSet.member?(groups, ":#{elem(key, 1)}"))
  end

  defp grouped?(_key, _groups), do: false

  defp root_key(key) when is_atom(key), do: key
  defp root_key(key) when is_tuple(key), do: elem(key, 0)
end
