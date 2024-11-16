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

  def operator_sup(operator_id) do
    gen_via(operator_id, "es")
  end

  def control_channel(operator_id) do
    gen_via(operator_id, "cc")
  end

  def hub(operator_id) do
    gen_id(operator_id, "hub") |> String.to_atom()
  end

  def stream_listener(operator_id, type) do
    case type do
      :primary -> gen_via(operator_id, "psl")
      :secondary -> gen_via(operator_id, "ssl")
    end
  end

  def stream_sup_primary(operator_id) do
    gen_via(operator_id, "scsp")
  end

  def stream_sup_secondary(operator_id) do
    gen_via(operator_id, "scss")
  end

  def control_unit(operator_id) do
    gen_via(operator_id, "cu")
  end

  def stream_channel_primary(operator_id, count_nr) do
    gen_via(operator_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(operator_id, count_nr) do
    gen_via(operator_id, "scs_" <> Integer.to_string(count_nr))
  end

  defp gen_via(operator_id, type_str) do
    {:via, Registry, {__MODULE__, gen_id(operator_id, type_str)}}
  end

  defp gen_id(operator_id, type_str) when is_atom(operator_id) do
    gen_id(Atom.to_string(operator_id), type_str)
  end

  defp gen_id(operator_id, type_str) do
    operator_id <> "_" <> type_str
  end
end
