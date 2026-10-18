defmodule Flezha.Config.Reference do
  @moduledoc """
  The configuration reference as the published pages print it: a view over the
  rows of `Flezha.Config.Keys`, as `Flezha.Config.Durations` is.

  `render_table/1` renders one group of rows as a Markdown table of name, type
  and default, sorted by name. A page carries each group between
  `<!-- config-keys:begin <group> -->` and `<!-- config-keys:end -->`, and a
  test holds the block equal to its rendering, so a row and its page cannot
  disagree. What a knob is for stays prose on the page.
  """

  alias Flezha.Config.Keys

  @type group :: :environment | :application_env | :runner | :build

  @runner_runtimes [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php]

  @doc "The groups a page can carry, in the order the reference lists them."
  @spec groups() :: [group()]
  def groups(), do: [:environment, :application_env, :runner, :build]

  @doc "The Markdown table of `group`'s rows, ending in a newline."
  @spec render_table(group()) :: binary()
  def render_table(group) do
    lines =
      for row <-
            Keys.rows()
            |> Map.values()
            |> Enum.filter(&member?(group, &1))
            |> Enum.sort_by(&name(group, &1)),
          do: "| `#{name(group, row)}` | #{type(group, row.type)} | #{default(group, row)} |"

    Enum.join(["| #{heading(group)} | Type | Default |", "|---|---|---|" | lines], "\n") <> "\n"
  end

  defp member?(:environment, row), do: row.env != nil and :controller in row.runtimes
  defp member?(:application_env, row), do: row.key != nil

  defp member?(:runner, row),
    do: row.env != nil and Enum.any?(row.runtimes, &(&1 in @runner_runtimes))

  defp member?(:build, row),
    do: row.env != nil and Enum.any?(row.runtimes, &(&1 in [:build, :shell]))

  defp heading(:application_env), do: "Key"
  defp heading(_group), do: "Variable"

  defp name(:application_env, row), do: key_name(row.key)
  defp name(_group, row), do: row.env

  # A keyword-list key is shown as `:parent[:child]`.
  defp key_name(key) when is_atom(key), do: inspect(key)

  defp key_name(key) do
    [root | path] = Tuple.to_list(key)
    inspect(root) <> Enum.map_join(path, &"[#{inspect(&1)}]")
  end

  defp type(:application_env, {:custom, :days_to_seconds}), do: "seconds"
  defp type(_group, type), do: type(type)

  defp type({:enum, values}), do: "one of " <> Enum.map_join(values, ", ", &"`#{&1}`")
  defp type({:list, of}), do: "list of " <> type(of)
  defp type({:custom, :days_to_seconds}), do: "days"
  defp type({:custom, :pem_files}), do: "PEM file paths"
  defp type(type), do: to_string(type)

  # The application env holds what the boot made of a variable: seconds where
  # the variable is days, and a term rather than its text.
  defp default(:application_env, %{type: {:custom, :days_to_seconds}, default: seconds}),
    do: "`#{seconds}`"

  # Sorted: OTP orders a small map's atom keys by when each atom was made.
  defp default(:application_env, %{type: :term, default: value}) when value != nil,
    do: "`#{inspect(value, custom_options: [sort_maps: true])}`"

  # A value is code; "unset", "derived" and the like are words.
  defp default(_group, %{default: d} = row)
       when d == nil or (is_tuple(d) and elem(d, 0) == :late),
       do: render_default(row)

  defp default(_group, row) do
    case render_default(row) do
      "" -> ~s(`""`)
      text -> "`#{text}`"
    end
  end

  @doc """
  The default as a user reads it in the configuration pages: the one rendering,
  so a page and a row cannot disagree about a value's spelling.
  """
  @spec render_default(Keys.name() | Keys.row()) :: binary()
  def render_default(name) when is_atom(name), do: render_default(Keys.fetch!(name))
  def render_default(%{default: {:late, _}}), do: "derived at boot"
  def render_default(%{default: {:env, var, _}}), do: var
  def render_default(%{per_run: p, default: nil}) when p != nil, do: "derived per run"
  def render_default(%{default: nil, derived: d}) when d != nil, do: "derived"
  def render_default(%{default: nil}), do: "unset"

  # The variable is days; the key it fills is seconds, because that is what
  # every reader of the key wants. The published default is the variable's unit.
  def render_default(%{type: {:custom, :days_to_seconds}, default: seconds}),
    do: to_string(div(seconds, 86_400))

  def render_default(%{default: [{reader, _} | _] = per_reader})
      when reader in [:controller, :runner],
      do: Enum.map_join(per_reader, ", ", fn {who, v} -> "#{v} (#{who})" end)

  def render_default(%{type: {:list, _}, default: list}) when is_list(list),
    do: Enum.map_join(list, ",", &to_string/1)

  def render_default(%{default: value}), do: to_string(value)
end
