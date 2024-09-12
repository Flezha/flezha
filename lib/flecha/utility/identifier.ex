defmodule Flecha.Utility.Identifier do
  # TODO: refactor.

  def engine_sup(engine_id) do
    (Atom.to_string(engine_id) <> "_engine_sup") |> String.to_atom()
  end

  def control_channel(engine_id) do
    (Atom.to_string(engine_id) <> "_control_channel") |> String.to_atom()
  end

  def stream_listener(engine_id) do
    (Atom.to_string(engine_id) <> "_listener") |> String.to_atom()
  end

  def stream_sup(engine_id) do
    (Atom.to_string(engine_id) <> "_stream_sup") |> String.to_atom()
  end
end
