defmodule Flecha do
  @moduledoc """
  Documentation for `FlechaCore`.
  """

  # TODO: Make configurable
  @call_timeout 1000

  def add_computation(computation) do
    GenServer.call(Flecha.Controller, {:add_computation, computation})
  end

  def add_io(computation_id, io_id, settings, direction) do
    computation_pid_lookup(computation_id)
    |> GenServer.call({:add_io, io_id, direction, settings}, @call_timeout)
  end

  def pipeline_io_internal_payload(computation_id, io_id, direction, payload) do
    computation_pid_lookup(computation_id)
    |> GenServer.call({:io_internal_payload, io_id, direction, payload}, @call_timeout)
  end

  defp computation_pid_lookup(computation_id) do
    case ProcessHub.child_lookup(:flecha_hub, computation_id) do
      {_, [{_, pid}]} ->
        pid

      _ ->
        raise "computation not found"
    end
  end
end
