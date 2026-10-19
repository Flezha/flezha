defmodule Flezha.Runner.Admission do
  @moduledoc """
  The checks a runner connect passes before its session is minted, in order.

  Each is a `Flezha.Runner.Admission.Check`. The `:caller` phase runs in the
  connecting process (`Flezha.Runner.FleetManager.connect_handler/4`); the
  `:manager` phase runs inside the fleet manager, because the identity pin and
  the drop mark must be atomic with the roster write. A check answers `:ok`,
  `{:ok, connect}` to hand the next check a connect it proved, or the refusal.
  """

  alias Flezha.AuthZ.RunnerProof
  alias Flezha.Runner.Admission.{IdentityPin, NotDropped, NotShuttingDown, Quota}

  @checks [RunnerProof, NotShuttingDown, Quota, IdentityPin, NotDropped]

  @spec run(:caller | :manager, map(), map()) :: {:ok, map()} | {:error, term()}
  def run(phase, connect, context) do
    @checks
    |> Enum.filter(&(&1.phase() == phase))
    |> Enum.reduce_while({:ok, connect}, fn check, {:ok, connect} ->
      case check.check(connect, context) do
        :ok -> {:cont, {:ok, connect}}
        {:ok, proven} -> {:cont, {:ok, proven}}
        {:error, _reason} = refusal -> {:halt, refusal}
      end
    end)
  end
end
