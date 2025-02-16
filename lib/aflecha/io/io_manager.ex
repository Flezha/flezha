defmodule Aflecha.IO.IOManager do
  alias Aflecha.Operator.FleetSupervisor
  alias Aflecha.Service.Identificator

  def add_io(operator_id, worker_id, io_struct, io_mode, opts) do
    timeout = Application.fetch_env!(:aflecha, :io_add_timeout)

    case worker_pid_lookup(operator_id, worker_id) do
      {:ok, pid} ->
        GenServer.call(pid, {:add_io, io_mode, io_struct, io_opts(opts)}, timeout)

      {:error, msg} ->
        {:error, msg}
    end
  end

  def send_io_payload(worker_id, io_id, io_mode, payload) do
    FleetSupervisor.default_operator()
    |> send_io_payload(worker_id, io_id, io_mode, payload)
  end

  def send_io_payload(operator_id, worker_id, io_id, io_mode, payload) do
    timeout = Application.fetch_env!(:aflecha, :io_payload_add_timeout)

    case worker_pid_lookup(operator_id, worker_id) do
      {:ok, pid} ->
        GenServer.call(pid, {:send_io_payload, io_id, io_mode, payload}, timeout)

      {:error, msg} ->
        {:error, msg}
    end
  end

  defp worker_pid_lookup(operator_id, worker_id) do
    case ProcessHub.child_lookup(Identificator.hub(operator_id), worker_id) do
      {_, [{_, pid}]} ->
        {:ok, pid}

      _ ->
        {:error, "worker not found"}
    end
  end

  defp io_opts(opts) do
    opts
    |> Keyword.put_new(:handling_type, :stream)
  end
end
