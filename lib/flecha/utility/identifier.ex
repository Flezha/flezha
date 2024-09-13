defmodule Flecha.Utility.Identifier do
  def engine_sup(engine_id) do
    gen_id(engine_id, "engine_sup")
  end

  def control_channel(engine_id) do
    gen_id(engine_id, "control_channel")
  end

  def stream_listener(engine_id) do
    gen_id(engine_id, "listener")
  end

  def stream_sup(engine_id) do
    gen_id(engine_id, "stream_sup")
  end

  def control_unit(engine_id) do
    gen_id(engine_id, "control_unit")
  end

  defp gen_id(engine_id, type_str) do
    (Atom.to_string(engine_id) <> "_" <> type_str) |> String.to_atom()
  end
end
