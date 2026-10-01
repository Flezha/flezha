defmodule Flezha.State.ScopePaths do
  @moduledoc """
  Where a stepped scope's durable state lives. A graph node that embeds a steps
  class runs one scope of it per edge key inside the outer instance; the scope's
  Cells, task journal and input log are keyed by its local id, which nests its
  directory under the outer instance's, so whatever moves, adopts or deletes the
  outer instance's tree carries its scopes.
  """

  alias Flezha.Operator.ClassRegistry
  alias Flezha.State.Paths

  @doc """
  The local id of a stepped scope `(instance, inner, key)`: its instance id on
  the runner and the class element of its Cells' identities. An absent key is
  an unkeyed edge's scope; the core derives the same id
  (`flz_core::operator::scope::scope_id`).
  """
  @spec scope_id(binary(), binary(), binary() | nil) :: binary()
  def scope_id(instance_id, inner, nil), do: instance_id <> "/" <> inner

  def scope_id(instance_id, inner, key),
    do: scope_id(instance_id, inner, nil) <> "/" <> Paths.encode_cell_key(key)

  @doc "A stepped scope's local id as its instance and `{inner, key}`; `:error` for an instance's."
  @spec split(binary()) :: {binary(), {binary(), binary() | nil}} | :error
  def split(id) do
    case :binary.split(id, "/", [:global]) do
      [outer, inner] -> {outer, {inner, nil}}
      [outer, inner, key] -> {outer, {inner, Paths.decode_cell_key(key)}}
      _instance -> :error
    end
  end

  @doc "The instance a stepped scope's local id belongs to; any other id is its own."
  @spec outer_of(binary()) :: binary()
  def outer_of(id), do: id |> :binary.split("/") |> hd()

  @doc "What a stepped scope's id carries after its instance's, `/<inner>[/<key>]`; empty for an instance."
  @spec suffix(binary()) :: binary()
  def suffix(id),
    do: binary_part(id, byte_size(outer_of(id)), byte_size(id) - byte_size(outer_of(id)))

  @doc """
  The local ids of the stepped scopes holding state under `instance_id`. A
  class whose bind declared no embedding holds none, and its directory is not
  read.
  """
  @spec scope_ids(binary()) :: [binary()]
  def scope_ids(instance_id) do
    if ClassRegistry.embeds?(instance_id),
      do: for({inner, key} <- scopes_of(instance_id), do: scope_id(instance_id, inner, key)),
      else: []
  end

  @doc """
  The stepped scopes `{inner, key}` holding a task journal under `instance_id`'s
  directory: the embedding node's own for an unkeyed edge, one per key under it
  otherwise.
  """
  @spec scopes_of(binary()) :: [{binary(), binary() | nil}]
  def scopes_of(instance_id) do
    for inner <- Paths.list_dir(Paths.class_dir(instance_id)),
        key <- if(journaled?(instance_id, inner, nil), do: [nil], else: keys(instance_id, inner)),
        key == nil or journaled?(instance_id, inner, key),
        do: {inner, key}
  end

  defp keys(instance_id, inner) do
    instance_id
    |> Paths.class_dir()
    |> Path.join(inner)
    |> Paths.list_dir()
    |> Enum.map(&Paths.decode_cell_key/1)
  end

  # A staged copy keeps a scope's journal under the scope's own id.
  defp journaled?(instance_id, inner, key) do
    id = scope_id(instance_id, inner, key)
    File.exists?(Paths.wal_path(put_elem(Paths.journal_id(Paths.adopting_for(id) || id), 0, id)))
  end
end
