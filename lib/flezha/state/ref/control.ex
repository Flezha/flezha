defmodule Flezha.State.Ref.Control do
  @moduledoc "`{:control, {store, writer}}`: DETS stores a replacement controller restarts from."

  use Flezha.State.Ref

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.State.{Ref, Store}

  # Each store group's layout subtree; the group name is durable (it rides a
  # manifest key). The files are each store's own `filename/0`, never a
  # directory scan: `bootstrap.token` lives beside them and must never leave
  # the node.
  @control_subtrees %{"authz" => :auth, "schedules" => :schedules}

  @doc "The store groups that ship."
  @spec stores() :: [binary()]
  def stores(), do: Map.keys(@control_subtrees)

  def payload_files({:control, {store, _writer}}),
    do: Ref.present(control_root(store), Store.filenames_in(subtree_of(store)))

  defp control_root(store), do: Layout.dir(subtree_of(store))

  defp subtree_of(store), do: Map.fetch!(@control_subtrees, store)
end
