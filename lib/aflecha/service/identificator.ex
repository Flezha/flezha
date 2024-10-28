defmodule Aflecha.Service.Identificator do
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
    gen_via(engine_id, "es")
  end

  def control_channel(engine_id) do
    gen_via(engine_id, "cc")
  end

  def stream_listener(engine_id, type) do
    case type do
      :primary -> gen_via(engine_id, "psl")
      :secondary -> gen_via(engine_id, "ssl")
    end
  end

  def stream_sup_primary(engine_id) do
    gen_via(engine_id, "scsp")
  end

  def stream_sup_secondary(engine_id) do
    gen_via(engine_id, "scss")
  end

  def control_unit(engine_id) do
    gen_via(engine_id, "cu")
  end

  def stream_channel_primary(engine_id, count_nr) do
    gen_via(engine_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(engine_id, count_nr) do
    gen_via(engine_id, "scs_" <> Integer.to_string(count_nr))
  end

  defp gen_via(engine_id, type_str) do
    {:via, Registry, {__MODULE__, gen_id(engine_id, type_str)}}
  end

  defp gen_id(engine_id, type_str) when is_atom(engine_id) do
    gen_id(Atom.to_string(engine_id), type_str)
  end

  defp gen_id(engine_id, type_str) do
    engine_id <> "_" <> type_str
  end
end
