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

  def engine_sup(subscriber_id) do
    gen_via(subscriber_id, "es")
  end

  def control_channel(subscriber_id) do
    gen_via(subscriber_id, "cc")
  end

  def stream_listener(subscriber_id, type) do
    case type do
      :primary -> gen_via(subscriber_id, "psl")
      :secondary -> gen_via(subscriber_id, "ssl")
    end
  end

  def stream_sup_primary(subscriber_id) do
    gen_via(subscriber_id, "scsp")
  end

  def stream_sup_secondary(subscriber_id) do
    gen_via(subscriber_id, "scss")
  end

  def control_unit(subscriber_id) do
    gen_via(subscriber_id, "cu")
  end

  def stream_channel_primary(subscriber_id, count_nr) do
    gen_via(subscriber_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(subscriber_id, count_nr) do
    gen_via(subscriber_id, "scs_" <> Integer.to_string(count_nr))
  end

  defp gen_via(subscriber_id, type_str) do
    {:via, Registry, {__MODULE__, gen_id(subscriber_id, type_str)}}
  end

  defp gen_id(subscriber_id, type_str) when is_atom(subscriber_id) do
    gen_id(Atom.to_string(subscriber_id), type_str)
  end

  defp gen_id(subscriber_id, type_str) do
    subscriber_id <> "_" <> type_str
  end
end
