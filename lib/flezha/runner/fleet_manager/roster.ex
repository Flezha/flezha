defmodule Flezha.Runner.FleetManager.Roster do
  @moduledoc """
  Reads over `state.runner_namespaces` — which runners a namespace holds and
  what status each is in.

  Every function is pure and takes the manager's state as a plain map, so the
  index can be asked a question without going through the GenServer that owns
  it. Invariant: a runner is `:active` in at most one namespace.
  """

  def get_runner_ids(state, namespace, status) do
    state
    |> Map.get(:runner_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {runner_id, {_, _}} -> runner_id end)
  end

  # A runner is healthy iff some namespace holds it with `:active` status.
  def runner_active?(state, runner_id) do
    state
    |> Map.get(:runner_namespaces, %{})
    |> Enum.any?(fn {_ns, runners} ->
      match?({:active, _}, Map.get(runners, runner_id))
    end)
  end

  def find_runner_namespace(state, runner_id) do
    Enum.find_value(state.runner_namespaces, fn {namespace, runners} ->
      if Map.has_key?(runners, runner_id), do: namespace, else: nil
    end)
  end
end
