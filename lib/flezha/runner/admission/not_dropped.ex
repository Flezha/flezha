defmodule Flezha.Runner.Admission.NotDropped do
  @moduledoc """
  Refuse a connect from a runner process an operator dropped.

  A standing mark (`Flezha.Runner.DropMarks`) on the connect's stable identity
  refuses the incarnation it names, on every controller, and the refusal is
  announced as `[:flezha, :runner, :drop_refused]` telemetry. Another
  incarnation is a different process: it is admitted, and once it is, the mark
  is lifted on every controller.
  """
  @behaviour Flezha.Runner.Admission.Check

  alias Flezha.Runner.DropMarks

  @impl true
  def phase, do: :manager

  @impl true
  def check(%{namespace: namespace, stable_id: stable_id} = connect, %{stream_ref: stream_ref})
      when is_binary(stable_id) do
    connecting = connect.incarnation

    case DropMarks.standing(namespace, stable_id) do
      nil ->
        :ok

      %{incarnation: ^connecting = incarnation} ->
        :telemetry.execute([:flezha, :runner, :drop_refused], %{count: 1}, %{
          namespace: namespace,
          stable_id: stable_id,
          incarnation: incarnation,
          pid: Flezha.Transport.Stream.peer_pid(stream_ref),
          node: node()
        })

        {:error, {:runner_dropped, namespace, stable_id}}

      _another_process ->
        :ok
    end
  end

  def check(_connect, _context), do: :ok
end
