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

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.AuthZ.RunnerProof
  alias Mix.Tasks.Bench.Fleet

  @namespace "bench"

  defp frame(secret), do: %{"namespace" => @namespace, "namespace_secret" => secret}

  # Issuing a credential is the task telling a user what it just did, so
  # the test that provokes the line owns it: captured, and asserted rather than
  # discarded, because a capture nobody reads would also pass if the line moved.
  defp issue_credential do
    {credential, printed} = with_io(fn -> Fleet.runner_credential(@namespace) end)

    assert printed =~ "Issued a runner credential for namespace #{@namespace}"

    credential
  end

  test "the environment the harness hands a runner proves the namespace it claims" do
    assert AuthZ.enabled?()

    assert [{"FLEZHA_NAMESPACE_SECRET", secret}] = issue_credential()

    assert {:ok, %{namespace: @namespace}} =
             RunnerProof.prove(nil, @namespace, frame(secret), :uds)
  end

  test "the credential an earlier run left behind is retired by the next one" do
    assert [{_var, first}] = issue_credential()
    assert [{_var, second}] = issue_credential()

    assert {:error, :namespace_not_proven} =
             RunnerProof.prove(nil, @namespace, frame(first), :uds)

    assert {:ok, %{namespace: @namespace}} =
             RunnerProof.prove(nil, @namespace, frame(second), :uds)
  end
end
