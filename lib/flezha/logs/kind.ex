defmodule Flezha.Logs.Kind do
  @moduledoc """
  One durable, append-only log, declared as data: what the shared log core
  needs to run it, and what a reader needs to show it.

    * `id` — the kind's name, an atom; its text is what callers name it by.
    * `title`, `description` — how a reader introduces it.
    * `layout` — the `Flezha.Layout.Table` subtree its files live in.
    * `prefix` — its file names: `<prefix>-current.jsonl` is the active file,
      `<prefix>-<UTC stamp>-<n>.jsonl[.gz]` a rotated one.
    * `keys` — where each setting comes from, by role: a `Flezha.Config.Keys`
      row, `{:duration, row}` for a `Flezha.Config.Durations` row, or
      `{:value, term}`. A role a kind does not declare reads `nil`.
    * `feed` — `{:bus, sink}` (a sink on the observability bus writes it) or
      `:direct` (the code that produces a record appends it).
    * `encode` — the record a written thing becomes: `{ts_us, fields}`, the
      wall-clock time and the ordered `{name, value}` pairs after `ts_iso`.
    * `filters` — `[%{field, label, type, values}]`, the fields a read may
      narrow by (`type` `"text"`, `"enum"` or `"time"`).
    * `columns` — `[%{field, label, type}]`, what a reader shows.
    * `children` — processes of the kind's own that start before its writer.
    * `events` — `source`, the module whose `emit/3` announces the kind's
      events, and the name each event (`rotated`, `swept`, `overloaded`) has
      on it.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Layout.Table, as: Layout

  @enforce_keys [:id, :title, :description, :layout, :prefix, :keys, :feed, :encode, :events]
  defstruct @enforce_keys ++ [filters: [], columns: [], children: []]

  @type t :: %__MODULE__{}

  @doc "The setting `role` of `kind`, or `nil` when the kind declares none."
  @spec get(t(), atom()) :: term()
  def get(%__MODULE__{keys: keys}, role) do
    case Map.get(keys, role) do
      nil -> nil
      {:value, value} -> value
      {:duration, row} -> Durations.get(row)
      key -> Config.get(key)
    end
  end

  @doc "The name the kind's `role` process registers under."
  @spec name(t() | atom(), atom()) :: {:via, Registry, term()}
  def name(%__MODULE__{id: id}, role), do: name(id, role)
  def name(id, role) when is_atom(id), do: {:via, Registry, {Flezha.Logs.Registry, {id, role}}}

  @doc "The kind's directory."
  @spec dir(t()) :: String.t()
  def dir(%__MODULE__{layout: layout}), do: Layout.dir(layout)

  @doc "The active file's name."
  @spec active(t()) :: String.t()
  def active(%__MODULE__{prefix: prefix}), do: prefix <> "-current.jsonl"

  @doc "Whether `name` is one of the kind's rotated files."
  @spec rotated?(t(), String.t()) :: boolean()
  def rotated?(%__MODULE__{prefix: prefix} = kind, name) when is_binary(name) do
    String.starts_with?(name, prefix <> "-") and name != active(kind) and
      (String.ends_with?(name, ".jsonl") or String.ends_with?(name, ".jsonl.gz"))
  end

  @doc "Announce one of the kind's events (`:rotated`, `:swept`, `:overloaded`) on its source."
  @spec emit(t(), atom(), map(), map()) :: :ok
  def emit(%__MODULE__{events: events}, event, measurements, metadata \\ %{}),
    do: events.source.emit(Map.fetch!(events, event), measurements, metadata)

  @doc "The filter fields a read of the kind may name."
  @spec filter_fields(t()) :: [String.t()]
  def filter_fields(%__MODULE__{filters: filters}), do: Enum.map(filters, & &1.field)
end
