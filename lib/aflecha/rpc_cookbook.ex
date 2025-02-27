defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Service.WorkerManager
  alias Aflecha.IO.IOManager

  @doc "Starts a worker."
  @spec worker_start(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def worker_start(operator_id, worker_id, factory_id, variables) do
    worker = %Aflecha.Worker.Builder{
      id: worker_id,
      factory_id: factory_id,
      variables: Aflecha.Service.Payloader.json_to_map(variables)
    }

    WorkerManager.start_worker(operator_id(operator_id), worker)
  end

  @doc "Stops a worker."
  @spec worker_stop(String.t(), String.t()) :: :ok | {:error, String.t()}
  def worker_stop(operator_id, worker_id) do
    WorkerManager.stop_worker(operator_id(operator_id), worker_id)
  end

  @doc "Adds an IO."
  def add_io(operator_id, worker_id, io_params, io_mode, opts) do
    io_struct = Aflecha.Service.Payloader.extract_io_settings(io_params)

    IOManager.add_io(operator_id(operator_id), worker_id, io_struct, io_mode, opts)
  end

  defp operator_id(operator_id) do
    case operator_id do
      "" -> Aflecha.Operator.FleetSupervisor.default_operator()
      _ -> operator_id
    end
  end
end
