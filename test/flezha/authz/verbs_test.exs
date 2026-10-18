defmodule Flezha.AuthZ.VerbsTest do
  @moduledoc """
  The declaration test. A bypass of the caller-verb seam is impossible
  because each clause here fails naming it: a published function that binds
  no row, a row naming an action the policy lacks, an action no row names, and
  a gated row that serves an unbound caller or reads its row before refusing.
  That no surface binds a row of its own is `Flezha.Service.Cli.CatalogueTest`'s.
  """
  # async: false: the auth stores are singletons and the posture is flipped.
  use FlezhaTest.Case, fixtures: [], async: false

  alias Flezha.AuthZ.{Action, Policy, Verb, Verbs}
  alias FlezhaTest.Helper.{AuthTestHelper, RpcSurface}

  @published "docs/internal/api/consumers/published.txt"
  @unbound %{source: :ui}
  @viewer %{source: :ui, principal: "vw@_system", role: :viewer, namespace: "_system"}

  test "every published function binds a row, and no context-taking function on a published module escapes one" do
    manifest = RpcSurface.manifest(@published)

    unbound =
      for {module, fun, arity, _where} <- manifest,
          not bound?(module, fun, arity),
          do: "#{inspect(module)}.#{fun}/#{arity}"

    assert unbound == [], "published but bound to no row: #{inspect(unbound)}"

    escaped =
      for module <- manifest |> Enum.map(&elem(&1, 0)) |> Enum.uniq(),
          {fun, arity} <- context_taking(module),
          not bound?(module, fun, arity),
          do: "#{inspect(module)}.#{fun}/#{arity}"

    assert escaped == [], "takes a caller context and binds no row: #{inspect(escaped)}"
  end

  test "every row names an action the policy knows, or is public" do
    strays =
      for row <- Verbs.all(),
          row.action != :public,
          not Action.known?(row.action),
          do: {row.verb, row.action}

    assert strays == []
  end

  test "every action is named by at least one row" do
    named = MapSet.new(Verbs.all(), & &1.action)
    orphans = Enum.reject(Action.all(), &MapSet.member?(named, &1))
    assert orphans == [], "actions no verb names: #{inspect(orphans)}"
  end

  describe "under :enforced" do
    setup do
      FlezhaTest.Case.install([
        {:auth_stores, prefix: "flz_verbs", enabled: true, claim: true, register_source: true}
      ])

      assert Flezha.AuthZ.enforcement_posture() == :enforced
      :ok
    end

    test "every gated row refuses an unbound caller, runs nothing, reads nothing, and is audited once" do
      for row <- gated() do
        boundary = System.monotonic_time(:microsecond)
        result = drive(row, @unbound)

        assert match?({:error, %Flezha.Error{class: :denied}}, result),
               "#{row.verb} served an unbound caller: #{inspect(result)}"

        refute_received {:acted, ^row}, "#{row.verb} ran its effect for an unbound caller"
        refute_received {:fetched, ^row}, "#{row.verb} read its row for an unbound caller"

        denials = AuthTestHelper.platform_authz_events(:authorization_denied, boundary)
        assert length(denials) == 1, "#{row.verb}: #{length(denials)} denials audited"
      end
    end

    test "a row-scoped verb refuses a role the action is not granted to before it reads the row" do
      for row <- gated(),
          match?({:row, _}, row.scope),
          not Policy.grants?(:viewer, row.action) do
        assert {:error, %Flezha.Error{code: "authz.role_insufficient"}} = drive(row, @viewer)
        refute_received {:fetched, ^row}, "#{row.verb} read its row for a viewer"
        refute_received {:acted, ^row}
      end
    end
  end

  defp gated, do: Enum.reject(Verbs.all(), &(&1.action == :public))

  # One namespace-shaped argument per position the scope rule can read; the
  # fetch and the effect each report to the test so a refusal that reached
  # either is caught.
  defp drive(row, caller_context) do
    test = self()

    Verb.run(
      row.verb,
      caller_context,
      args_for(row),
      fn _call -> send(test, {:acted, row}) end,
      row: fn _args ->
        send(test, {:fetched, row})
        {:ok, %{namespace: "default"}}
      end
    )
  end

  defp args_for(%{scope: {:argument, n}}), do: List.duplicate("default", n)
  defp args_for(_row), do: []

  defp bound?(module, fun, arity),
    do: Enum.any?(module.__verbs__(), &match?({^fun, ^arity, _verb, _opts}, &1))

  # Every documented public function whose last parameter is the caller
  # context, as its head spells it. A `@doc false` function is an internal
  # helper; the manifest test above keeps it off the published surface.
  defp context_taking(module) do
    for {{:function, fun, arity}, _anno, signatures, doc, _meta} <- docs(module),
        doc != :hidden,
        Enum.any?(signatures, &String.match?(&1, ~r/caller_context\)$/)),
        do: {fun, arity}
  end

  defp docs(module) do
    {:docs_v1, _anno, _lang, _format, _doc, _meta, entries} = Code.fetch_docs(module)
    entries
  end
end
