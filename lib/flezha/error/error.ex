defmodule Flezha.Error do
  @moduledoc """
  The one shape a failure reaches a caller in, on every surface: a stable
  `code` spelled `family.snake`, a `class`, a plain `message`, the `details`
  the message names, and a `hint` naming the next command when there is one.

  Reason terms stay the controller's own vocabulary. `from_reason/2` turns one
  into this struct once, where it leaves for a caller: at
  `Flezha.AuthZ.Verb.run/5` for every verb and published function, and at the
  declared wire sites (`"wire.spawn"`, `"wire.connect"`, `"wire.commit"`,
  `"wire.message"`) for what a runner is answered. The catalogue is
  `flz_transport/errors.json`. The rendering is `Flezha.SDK.Error`'s, the one
  Elixir renderer, which the Rust one matches byte for byte
  (`test/fixtures/error_rendering.json`).
  """

  alias Flezha.SDK.Error, as: Renderer

  @external_resource Path.expand("../../../flz_transport/errors.json", __DIR__)
  @catalogue Renderer.catalogue()

  @uncatalogued "internal.uncatalogued"

  defstruct [:code, :class, :message, details: %{}, hint: nil]

  @type class ::
          :internal | :invalid | :unreachable | :denied | :not_found | :conflict | :unavailable

  @type t :: %__MODULE__{
          code: String.t(),
          class: class(),
          message: String.t(),
          details: %{optional(String.t()) => term()},
          hint: String.t() | nil
        }

  @codes @catalogue["codes"]

  # `{prefix, arity}` -> [{sites, code, args}], where `prefix` is the reason's
  # leading atoms, `arity` the positions after them, and `sites` the verb
  # actions or wire sites an entry is limited to (nil: anywhere).
  @reasons (for {code, entry} <- @codes,
                reason <- Map.get(entry, "reasons", []),
                reduce: %{} do
              acc ->
                prefix = reason["tag"] |> List.wrap() |> Enum.map(&String.to_atom/1)
                args = Map.get(reason, "args", [])
                key = {prefix, length(args)}

                Map.update(
                  acc,
                  key,
                  [{reason["at"], code, args}],
                  &[{reason["at"], code, args} | &1]
                )
            end)

  @doc "The catalogue as it was compiled in: classes, sites, codes and the internal list."
  @spec catalogue() :: map()
  def catalogue(), do: @catalogue

  @doc """
  The error `reason` means at `site` (a verb action or a wire site). An error
  is answered as it is. A reason the catalogue does not map becomes
  `internal.uncatalogued`, carrying the reason as text, and emits
  `[:flezha, :error, :uncatalogued]`.
  """
  @spec from_reason(term(), atom() | String.t() | nil) :: t()
  def from_reason(%__MODULE__{} = error, _site), do: error

  def from_reason(reason, site) do
    site = if is_atom(site) and not is_nil(site), do: Atom.to_string(site), else: site

    case lookup(reason, site) do
      {code, args, values} -> new(code, details(args, values))
      nil -> uncatalogued(reason, site)
    end
  end

  @doc "Whether the catalogue maps `reason` at `site`, as `from_reason/2` would convert it."
  @spec catalogued?(term(), atom() | String.t() | nil) :: boolean()
  def catalogued?(%__MODULE__{}, _site), do: true

  def catalogued?(reason, site) do
    site = if is_atom(site) and not is_nil(site), do: Atom.to_string(site), else: site
    lookup(reason, site) != nil
  end

  @doc """
  The error `code` names, its templates filled from `details`. A code the
  catalogue does not know is `internal.uncatalogued`, naming it.
  """
  @spec new(String.t(), map() | keyword()) :: t()
  def new(code, details \\ %{}) do
    if Map.has_key?(@codes, code),
      do: struct(__MODULE__, Map.from_struct(Renderer.new(code, details))),
      else: uncatalogued({:unknown_code, code}, nil)
  end

  @doc "`:text` is `error: <message> (<code>)` and a `hint:` line; `:json` the one-line document."
  @spec render(t(), :text | :json | String.t()) :: String.t()
  defdelegate render(error, format), to: Renderer

  @doc "The `{code, class, message, details, hint}` object, as JSON with a fixed key order."
  @spec object(t()) :: String.t()
  defdelegate object(error), to: Renderer

  @doc "`details` as the compact, key-sorted JSON object the wire carries."
  @spec details_json(t()) :: String.t()
  defdelegate details_json(error), to: Renderer

  @doc "The error as the wire's `Error` message, which every runner reply carries."
  @spec to_wire(t()) :: RunnerCommands.Error.t()
  def to_wire(%__MODULE__{} = error) do
    %RunnerCommands.Error{
      code: error.code,
      class: Atom.to_string(error.class),
      message: error.message,
      details_json: details_json(error),
      hint: error.hint || ""
    }
  end

  @doc """
  The error a runner answered with on the wire, as it rendered it from the same
  catalogue; `nil` for a message that carries none.
  """
  @spec from_wire(RunnerCommands.Error.t() | nil) :: t() | nil
  def from_wire(nil), do: nil
  def from_wire(%{code: ""}), do: nil

  def from_wire(wire), do: struct(__MODULE__, Map.from_struct(Renderer.from_wire(wire)))

  @doc "The exit code a `flz` verb ends with for this error: the class's."
  @spec exit_code(t()) :: pos_integer()
  defdelegate exit_code(error), to: Renderer

  @doc """
  Leave the current verb body with `reason`, which `Flezha.AuthZ.Verb.run/5`
  answers as `{:error, reason}` converted at the verb's site. For a failure
  found deep inside a body, where returning it would thread it through every
  caller.
  """
  @spec throw!(term()) :: no_return()
  def throw!(reason), do: throw({__MODULE__, reason})

  # ---------- mapping ----------

  defp lookup(reason, site) do
    Enum.find_value(keys(reason), fn {prefix, values} ->
      case Map.get(@reasons, {prefix, length(values)}) do
        nil -> nil
        entries -> pick(entries, site, values)
      end
    end)
  end

  # The reason's candidate keys, the longest atom prefix first: `{:forbidden,
  # :role_insufficient}` is tried as that pair before as `:forbidden` with one
  # argument.
  defp keys(reason) when is_atom(reason) and not is_nil(reason) and not is_boolean(reason),
    do: [{[reason], []}]

  defp keys(reason) when is_tuple(reason) and tuple_size(reason) > 0 do
    case Tuple.to_list(reason) do
      [tag, sub | rest] when is_atom(tag) and is_atom(sub) and not is_nil(sub) ->
        [{[tag, sub], rest}, {[tag], [sub | rest]}]

      [tag | rest] when is_atom(tag) ->
        [{[tag], rest}]

      _ ->
        []
    end
  end

  defp keys(_reason), do: []

  # An entry limited to this site beats one limited to none.
  defp pick(entries, site, values) do
    at_site = Enum.find(entries, fn {sites, _, _} -> is_list(sites) and site in sites end)
    anywhere = Enum.find(entries, fn {sites, _, _} -> is_nil(sites) end)

    case at_site || anywhere do
      {_sites, code, args} -> {code, args, values}
      nil -> nil
    end
  end

  # A position named `_` is dropped; a map in any position merges its keys.
  defp details(args, values) do
    args
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {"_", _value}, acc -> acc
      {_name, %{} = map}, acc when not is_struct(map) -> Map.merge(acc, stringify_keys(map))
      {name, value}, acc -> Map.put(acc, name, value)
    end)
  end

  defp uncatalogued(reason, site) do
    :telemetry.execute([:flezha, :error, :uncatalogued], %{count: 1}, %{
      reason: reason,
      site: site
    })

    new(@uncatalogued, %{"reason" => inspect(reason)})
  end

  defp stringify_keys(map), do: Map.new(map, fn {key, value} -> {to_string(key), value} end)
end

defimpl Jason.Encoder, for: Flezha.Error do
  # An error embedded in a JSON answer is the same object the document
  # carries, in the same key order.
  def encode(error, _opts), do: Flezha.Error.object(error)
end
