defmodule Flezha.Plugin.Host.MsgId do
  @moduledoc """
  The `flz.msg_id` a source stamps on a record, `"<plugin_id>/<position>"`: the
  id the relay's input log absorbs a record read again by. An id the sender
  set wins.

  Macros, so a record costs what a hand-written map costs: on a map literal the
  id is one more key of that literal, and on any other map it is one lookup and
  one put, inline. A position written as a list (`[relation | cursor]`) is
  joined with the plugin id in one `Enum.join/2`. This module names nothing, so
  requiring it adds no compile-time dependency.
  """

  @key "flz.msg_id"

  @doc "`metadata` carrying the record's id at `position`."
  defmacro stamp(metadata, plugin_id, position),
    do: stamp_ast(metadata, id_ast(plugin_id, position))

  @doc "`stamp/3` for a record that may have no position: `nil` stamps no id."
  defmacro stamp_some(metadata, plugin_id, position) do
    present = Macro.unique_var(:position, __MODULE__)

    quote do
      case unquote(position) do
        nil -> unquote(metadata)
        unquote(present) -> unquote(stamp_ast(metadata, id_ast(plugin_id, present)))
      end
    end
  end

  defp id_ast(plugin_id, [_ | _] = parts),
    do: quote(do: Enum.join([unquote(plugin_id) | unquote(parts)], "/"))

  defp id_ast(plugin_id, position),
    do: quote(do: "#{unquote(plugin_id)}/#{unquote(position)}")

  # A map update (`%{map | key => value}`) is no literal: its map may hold an id.
  defp stamp_ast({:%{}, _meta, [{:|, _, _}]} = update, id), do: put_unless_set(update, id)

  defp stamp_ast({:%{}, meta, pairs}, id) do
    if List.keymember?(pairs, @key, 0),
      do: raise(ArgumentError, "a map literal that names #{@key} is stamped already")

    {:%{}, meta, pairs ++ [{@key, id}]}
  end

  defp stamp_ast(metadata, id), do: put_unless_set(metadata, id)

  defp put_unless_set(metadata, id) do
    quote do
      case unquote(metadata) do
        %{unquote(@key) => _} = stamped -> stamped
        unstamped -> Map.put(unstamped, unquote(@key), unquote(id))
      end
    end
  end
end
