defmodule Aflecha.IO.IOManager do
  alias Aflecha.Operator.OperatorsSup
  alias Aflecha.Service.Identificator

  def add_io(operator_id, computation_id, io_struct, io_mode, opts) do
    timeout = Application.fetch_env!(:aflecha, :io_add_timeout)

    computation_pid_lookup(operator_id, computation_id)
    |> GenServer.call({:add_io, io_mode, io_struct, io_opts(opts)}, timeout)
  end

  def send_io_payload(computation_id, io_id, io_mode, payload) do
    OperatorsSup.default_operator()
    |> send_io_payload(computation_id, io_id, io_mode, payload)
  end

  def send_io_payload(operator_id, computation_id, io_id, io_mode, payload) do
    timeout = Application.fetch_env!(:aflecha, :io_payload_add_timeout)

    computation_pid_lookup(operator_id, computation_id)
    |> GenServer.call({:send_io_payload, io_id, io_mode, payload}, timeout)
  end

  defp computation_pid_lookup(operator_id, computation_id) do
    case ProcessHub.child_lookup(Identificator.hub(operator_id), computation_id) do
      {_, [{_, pid}]} ->
        pid

      _ ->
        raise "computation not found"
    end
  end

  defp io_opts(opts) do
    opts
    |> Keyword.put_new(:handling_type, :stream)
  end
end
