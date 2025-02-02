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

  def unique_id(group_id) do
    rand_nums =
      Enum.reduce(1..4, "", fn _x, acc ->
        rand_num =
          Enum.random(1..1000)
          |> Integer.to_string()

        rand_num <> acc
      end)

    id = group_id <> "_" <> rand_nums

    case Registry.lookup(__MODULE__, id) do
      [] -> id
      _ -> unique_id(group_id)
    end
  end

  def unit_sup(group_id) do
    gen_via(group_id, "wus")
  end

  def control_channel(group_worker_id) do
    gen_via(group_worker_id, "cc")
  end

  def hub(group_id) do
    gen_id(group_id, "hub") |> String.to_atom()
  end

  def stream_listener(worker_group_id, type) do
    case type do
      :primary -> gen_via(worker_group_id, "psl")
      :secondary -> gen_via(worker_group_id, "ssl")
    end
  end

  def stream_sup_primary(group_id) do
    gen_via(group_id, "scsp")
  end

  def stream_sup_secondary(group_id) do
    gen_via(group_id, "scss")
  end

  def control_unit(group_worker_id) do
    gen_via(group_worker_id, "cu")
  end

  def stream_channel_primary(group_id, count_nr) do
    gen_via(group_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(group_id, count_nr) do
    gen_via(group_id, "scs_" <> Integer.to_string(count_nr))
  end

  defp gen_via(prefix, suffix) do
    {:via, Registry, {__MODULE__, gen_id(prefix, suffix)}}
  end

  # defp gen_via(group_id, group_worker_id, type_str) do
  #   {:via, Registry, {__MODULE__, gen_id(group_id, group_worker_id, type_str)}}
  # end

  defp gen_id(group_id, type_str) when is_atom(group_id) do
    gen_id(Atom.to_string(group_id), type_str)
  end

  defp gen_id(group_id, type_str) do
    group_id <> "_" <> type_str
  end

  # defp gen_id(group_id, group_worker_id, type_str) do
  #   group_id <> "_" <> group_worker_id <> "_" <> type_str
  # end
end
