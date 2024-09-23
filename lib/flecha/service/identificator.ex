defmodule Flecha.Service.Identificator do
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(_) do
    Registry.start_link(
      keys: :unique,
      partitions: 1,
      name: __MODULE__
    )
  end

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

  defp gen_id(engine_id, type_str) when is_atom(engine_id) do
    gen_id(Atom.to_string(engine_id), type_str)
  end

  defp gen_id(engine_id, type_str) do
    id = (engine_id <> "_" <> type_str) |> String.to_atom()

    {:via, Registry, {__MODULE__, id}}
  end
end
