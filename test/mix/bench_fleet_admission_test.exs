defmodule Mix.Tasks.Bench.FleetAdmissionTest do
  @moduledoc """
  The admission precondition the bench harness establishes for itself.

  A bench runs under the prod posture, so on a box whose bootstrap has been
  claimed its runners are admitted only if the harness hands them what the
  connect gate asks for. No other suite can see that: they all run with
  enforcement off. This one holds the harness against `Flezha.AuthZ.RunnerProof`,
  the gate a connect passes first.
  """
  # async: false: the auth stores are a shared on-disk root.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_bench_admission", claim: true}],
    async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.RunnerProof
  alias Mix.Tasks.Bench.Fleet

  @namespace "bench"

  defp frame(secret), do: %{"namespace" => @namespace, "namespace_secret" => secret}

  test "the environment the harness hands a runner proves the namespace it claims" do
    assert AuthZ.enabled?()

    assert [{"FLEZHA_NAMESPACE_SECRET", secret}] = Fleet.runner_credential(@namespace)

    assert {:ok, %{namespace: @namespace}} =
             RunnerProof.prove(nil, @namespace, frame(secret), :uds)
  end

  test "the credential an earlier run left behind is retired by the next one" do
    assert [{_var, first}] = Fleet.runner_credential(@namespace)
    assert [{_var, second}] = Fleet.runner_credential(@namespace)

    assert {:error, :namespace_not_proven} =
             RunnerProof.prove(nil, @namespace, frame(first), :uds)

    assert {:ok, %{namespace: @namespace}} =
             RunnerProof.prove(nil, @namespace, frame(second), :uds)
  end
end
