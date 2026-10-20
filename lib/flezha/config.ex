defmodule Flezha.Config do
  @moduledoc """
  The one reader of a configuration knob.

  Every name is a row of `Flezha.Config.Keys`; this module answers what a site
  gets for one: the user's application-env override, else the row's
  derivation over another row, else its declared default. It never reads the OS
  environment — `Flezha.Config.Environment` translated the variables into keys
  at boot, and after that the key is the only source, so one process cannot
  answer differently from another because it was started with a different
  environment.

  A runner is a different process and reads its own: `Flezha.SDK.Knob` for the
  Elixir SDK, and `flz_transport::env` for the crates and, through the core's
  knob entry, every other SDK. They share the names in the table, never this
  reader.
  """

  alias Flezha.Config.Keys

  @doc "What `name` is configured to be."
  @spec get(Keys.name()) :: term()
  def get(name), do: resolve(Keys.fetch!(name), &app_env/1)

  @doc """
  The declared default alone, with no override — what a test asserts against,
  and how a reader whose default differs from the controller's asks for its own
  (`default(:log_level, :runner)`).
  """
  @spec default(Keys.name(), atom()) :: term()
  def default(name, reader \\ :controller), do: declared(Keys.fetch!(name), reader)

  defp declared(%{default: nil, derived: nil}, _reader), do: nil
  defp declared(%{default: nil} = row, _reader), do: resolve(row, fn _ -> nil end)
  defp declared(%{default: value}, reader), do: literal(value, reader)

  @doc """
  Every row filed under one compound key, as the keyword list a subsystem takes
  its options as — so a subsystem gains a setting by gaining a row.
  """
  @spec group(atom() | tuple()) :: keyword()
  def group(prefix) do
    path = if is_tuple(prefix), do: Tuple.to_list(prefix), else: [prefix]
    depth = length(path)

    for {name, row} <- Keys.rows(),
        is_tuple(row.key),
        tuple_size(row.key) == depth + 1,
        Enum.take(Tuple.to_list(row.key), depth) == path,
        do: {elem(row.key, depth), get(name)}
  end

  # ---------- resolution ----------

  # One walk, three readers: an override wins, a derivation reads its parent
  # through the SAME reader, and the declared default is the floor.
  defp resolve(row, override) do
    case override.(row) do
      nil -> derive(row, override)
      value -> value
    end
  end

  defp derive(%{derived: nil, default: default}, _override), do: literal(default, :controller)

  defp derive(%{derived: {:factor, parent, k}}, override),
    do: trunc(resolve(Keys.fetch!(parent), override) * k)

  defp derive(%{derived: {:headroom, parent, delta}}, override),
    do: resolve(Keys.fetch!(parent), override) + delta

  defp derive(%{derived: {:one_of, parent}}, override),
    do: [resolve(Keys.fetch!(parent), override)]

  defp derive(%{derived: {:join, parent, leaf}}, override),
    do: Path.join(resolve(Keys.fetch!(parent), override), leaf)

  @doc """
  A declared default as a value: a `{:late, mfa}` answered now, a
  `{:env, var, fallback}` without its variable, a per-reader pair on `reader`'s
  side, anything else as it stands.
  """
  @spec literal(term(), atom()) :: term()
  def literal(value, reader \\ :controller)
  def literal({:late, {module, function, args}}, _reader), do: apply(module, function, args)
  def literal({:env, _var, fallback}, _reader), do: fallback

  def literal([{first, _} | _] = per_reader, reader) when first in [:controller, :runner],
    do: Keyword.fetch!(per_reader, reader)

  def literal(value, _reader), do: value

  defp app_env(%{key: nil}), do: nil
  defp app_env(%{key: key}) when is_atom(key), do: Application.get_env(:flezha, key)

  defp app_env(%{key: {group, sub}}),
    do: :flezha |> Application.get_env(group, []) |> Keyword.get(sub)

  defp app_env(%{key: {group, sub, leaf}}) do
    :flezha
    |> Application.get_env(group, [])
    |> Keyword.get(sub, [])
    |> Keyword.get(leaf)
  end

  @doc "What `name` is configured to be. Raises when nothing configures one."
  @spec fetch!(Keys.name()) :: term()
  def fetch!(name) do
    case get(name) do
      nil -> raise ArgumentError, "#{name} has no value and no default#{hint(name)}"
      value -> value
    end
  end

  defp hint(name) do
    case Keys.env_name(name) do
      nil -> ""
      var -> "; set #{var}"
    end
  end

  @doc """
  The environment variable a knob is set with, for a refusal message. The knob
  is named by its row name, or by the application-env key it fills where the
  caller holds one of those instead.
  """
  @spec env_name(Keys.name() | Keys.key()) :: binary() | nil
  defdelegate env_name(name), to: Keys
end
