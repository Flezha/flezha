defmodule Flezha.Config.Reference do
  @moduledoc """
  The configuration reference the published pages print, a view over the rows of
  `Flezha.Config.Keys`. A page holds each group's `render_table/1` between
  `<!-- config-keys:begin <group> -->` and `<!-- config-keys:end -->`, and a test
  holds the block equal to it; what a knob is for stays prose on the page.
  """

  alias Flezha.Config.Keys

  @type group :: :environment | :application_env | :runner | :build

  @doc "The Markdown table of `group`'s rows, sorted by name, ending in a newline."
  @spec render_table(group()) :: binary()
  def render_table(group) do
    rows = Keys.rows() |> Map.values() |> Enum.filter(&member?(group, &1))
    head = if group == :application_env, do: "Key", else: "Variable"

    for row <- Enum.sort_by(rows, &name(group, &1)),
        into: "| #{head} | Type | Default |\n|---|---|---|\n",
        do: "| `#{name(group, row)}` | #{type(group, row.type)} | #{default(group, row)} |\n"
  end

  defp member?(:application_env, row), do: row.key != nil
  defp member?(:environment, row), do: reads?(row, [:controller])

  defp member?(:runner, row),
    do: reads?(row, [:elixir_sdk, :rust, :java, :node, :python, :ruby, :php, :go])

  defp member?(:build, row), do: reads?(row, [:build, :shell])

  defp reads?(row, runtimes), do: row.env && Enum.any?(row.runtimes, &(&1 in runtimes))

  # A keyword-list key is shown as `:parent[:child]`.
  defp name(:application_env, %{key: key}) do
    [root | path] = if is_tuple(key), do: Tuple.to_list(key), else: [key]
    inspect(root) <> Enum.map_join(path, &"[#{inspect(&1)}]")
  end

  defp name(_group, row), do: row.env

  defp type(:application_env, {:custom, :days_to_seconds}), do: "seconds"
  defp type(_group, {:enum, values}), do: "one of " <> Enum.map_join(values, ", ", &"`#{&1}`")
  defp type(group, {:list, of}), do: "list of " <> type(group, of)
  defp type(_group, {:custom, :days_to_seconds}), do: "days"
  defp type(_group, {:custom, :pem_files}), do: "PEM file paths"
  defp type(_group, type), do: to_string(type)

  # The application env holds what the boot made of a variable: seconds where
  # the variable is days, and a term (maps sorted, as OTP orders a small map's
  # atom keys by when each atom was made) rather than its text.
  defp default(:application_env, %{type: {:custom, :days_to_seconds}, default: s}), do: "`#{s}`"

  defp default(:application_env, %{type: :term, default: value}) when value != nil,
    do: "`#{inspect(value, custom_options: [sort_maps: true])}`"

  # A value is code; "unset", "derived" and the like are words.
  defp default(_group, %{default: d} = row) when d == nil or elem(d, 0) == :late,
    do: render_default(row)

  defp default(_group, row), do: "`#{with "" <- render_default(row), do: ~s("")}`"

  @doc "The default as the configuration pages spell it, so a page and a row agree."
  @spec render_default(Keys.row()) :: binary()
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
