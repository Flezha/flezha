defmodule Aflecha do
  alias Aflecha.Computation.Service.Activator
  alias Aflecha.Operator.Controller
  alias Aflecha.Operator.OperatorsSup
  alias Aflecha.Service.Identificator
  alias Aflecha.Computation.Builder, as: ComputationBuilder

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO: the default no operator parameter is bringing lots of duplication and mess.

  def computation_state(computation_id) do
    OperatorsSup.default_operator()
    |> computation_state(computation_id)
  end

  def computation_state(operator_id, computation_id) do
    GenServer.call(Controller, {:get_computation_state, operator_id, computation_id})
  end

  def start_computation(operator_id, computation) do
    Activator.start_computation(operator_id, computation)
  end

  def stop_computation(operator_id, computation_id) do
    Activator.stop_computation(operator_id, computation_id)
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

  def add_output(computation_id, settings, opts) do
    add_output(
      OperatorsSup.default_operator(),
      computation_id,
      settings,
      opts
    )
  end

  def add_output(operator_id, computation_id, settings, opts) do
    add_io(operator_id, computation_id, settings, :output, opts)
  end

  def add_input(computation_id, settings, opts) do
    add_input(
      OperatorsSup.default_operator(),
      computation_id,
      settings,
      opts
    )
  end

  def add_input(operator_id, computation_id, settings, opts) do
    add_io(operator_id, computation_id, settings, :input, opts)
  end

  def add_io(operator_id, computation_id, io_struct, direction, opts) do
    timeout = Application.fetch_env!(:aflecha, :io_add_timeout)

    computation_pid_lookup(operator_id, computation_id)
    |> GenServer.call({:add_io, direction, io_struct, io_opts(opts)}, timeout)
  end

  def send_io_payload(computation_id, io_id, direction, payload) do
    OperatorsSup.default_operator()
    |> send_io_payload(computation_id, io_id, direction, payload)
  end

  def send_io_payload(operator_id, computation_id, io_id, direction, payload) do
    timeout = Application.fetch_env!(:aflecha, :io_payload_add_timeout)

    computation_pid_lookup(operator_id, computation_id)
    |> GenServer.call({:send_io_payload, io_id, direction, payload}, timeout)
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
