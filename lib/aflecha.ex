defmodule Aflecha do
  alias Aflecha.Subscriber.SubscribersSupervisor
  alias Aflecha.Service.Identificator
  alias Aflecha.Computation.Builder, as: ComputationBuilder

  @moduledoc """
  Documentation for `Aflecha`.
  """

  def add_computation(computation) do
    SubscribersSupervisor.default_group()
    |> add_computation(computation)
  end

  def add_computation(group_id, computation) do
    GenServer.call(Aflecha.Subscriber.Controller, {:add_computation, group_id, computation})
  end

  def rem_computation(computation_id) do
    SubscribersSupervisor.default_group()
    |> rem_computation(computation_id)
  end

  def rem_computation(group_id, computation_id) do
    GenServer.call(Aflecha.Subscriber.Controller, {:rem_computation, group_id, computation_id})
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
    SubscribersSupervisor.default_group()
    |> add_io(computation_id, io_id, settings, direction)
  end

  def add_io(group_id, computation_id, io_id, settings, direction) do
    timeout = Application.fetch_env!(:aflecha, :io_add_timeout)

    computation_pid_lookup(group_id, computation_id)
    |> GenServer.call({:add_io, io_id, direction, settings}, timeout)
  end

  def pipeline_io_internal_payload(computation_id, io_id, direction, payload) do
    SubscribersSupervisor.default_group()
    |> pipeline_io_internal_payload(computation_id, io_id, direction, payload)
  end

  def pipeline_io_internal_payload(group_id, computation_id, io_id, direction, payload) do
    timeout = Application.fetch_env!(:aflecha, :io_payload_add_timeout)

    computation_pid_lookup(group_id, computation_id)
    |> GenServer.call({:io_internal_payload, io_id, direction, payload}, timeout)
  end

  defp computation_pid_lookup(group_id, computation_id) do
    case ProcessHub.child_lookup(Identificator.hub(group_id), computation_id) do
      {_, [{_, pid}]} ->
        pid

      _ ->
        raise "computation not found"
    end
  end
end
