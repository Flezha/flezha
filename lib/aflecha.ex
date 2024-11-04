defmodule Aflecha do
  alias Aflecha.Computation.Builder, as: ComputationBuilder

  @moduledoc """
  Documentation for `Aflecha`.
  """

  def add_computation(computation) do
    GenServer.call(Aflecha.Controller, {:add_computation, computation})
  end

  def rem_computation(computation_id) do
    GenServer.call(Aflecha.Controller, {:rem_computation, computation_id})
  end

  def construct_computation!(input, type) do
    result =
      case type do
        :json -> ComputationBuilder.from_json(input)
        :map -> ComputationBuilder.from_map(input)
      end

    case result do
      {:ok, computation} -> computation
      {:error, reason} -> raise reason
    end
  end

  def add_io(computation_id, io_id, settings, direction) do
    timeout = Application.fetch_env!(:aflecha, :io_add_timeout)

    computation_pid_lookup(computation_id)
    |> GenServer.call({:add_io, io_id, direction, settings}, timeout)
  end

  def pipeline_io_internal_payload(computation_id, io_id, direction, payload) do
    timeout = Application.fetch_env!(:aflecha, :io_payload_add_timeout)

    computation_pid_lookup(computation_id)
    |> GenServer.call({:io_internal_payload, io_id, direction, payload}, timeout)
  end

  defp computation_pid_lookup(computation_id) do
    case ProcessHub.child_lookup(:aflecha_hub, computation_id) do
      {_, [{_, pid}]} ->
        pid

      _ ->
        raise "computation not found"
    end
  end
end
