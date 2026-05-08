defmodule Flezha.Service.Identificator do
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

  def unique_id(namespace) do
    rand_nums =
      Enum.reduce(1..4, "", fn _x, acc ->
        rand_num =
          Enum.random(1..1000)
          |> Integer.to_string()

        rand_num <> acc
      end)

    id = namespace <> "_" <> rand_nums

    case Registry.lookup(__MODULE__, id) do
      [] -> id
      _ -> unique_id(namespace)
    end
  end

  def unit_sup(executor_id) do
    gen_via(executor_id, "wus")
  end

  def control_channel(executor_id) do
    gen_via(executor_id, "cc")
  end

  def hub(namespace) do
    gen_id(namespace, "hub") |> String.to_atom()
  end

  def stream_listener(executor_id, type) do
    case type do
      :primary -> gen_via(executor_id, "psl")
      :secondary -> gen_via(executor_id, "ssl")
    end
  end

  def stream_sup_primary(executor_id) do
    gen_via(executor_id, "scsp")
  end

  def stream_sup_secondary(executor_id) do
    gen_via(executor_id, "scss")
  end

  def control_unit(executor_id) do
    gen_via(executor_id, "cu")
  end

  def stream_channel_primary(executor_id, count_nr) do
    gen_via(executor_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(executor_id, count_nr) do
    gen_via(executor_id, "scs_" <> Integer.to_string(count_nr))
  end

  defp gen_via(prefix, suffix) do
    {:via, Registry, {__MODULE__, gen_id(prefix, suffix)}}
  end

  # defp gen_via(executor_id, executor_id, type_str) do
  #   {:via, Registry, {__MODULE__, gen_id(executor_id, executor_id, type_str)}}
  # end

  defp gen_id(executor_id, type_str) when is_atom(executor_id) do
    gen_id(Atom.to_string(executor_id), type_str)
  end

  defp gen_id(executor_id, type_str) do
    executor_id <> "_" <> type_str
  end

  # defp gen_id(executor_id, executor_id, type_str) do
  #   executor_id <> "_" <> executor_id <> "_" <> type_str
  # end
end
