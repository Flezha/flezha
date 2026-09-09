defmodule Flezha.AuthZ.VerbsTest do
  @moduledoc """
  The declaration test. A bypass of the operator-verb seam is impossible
  because each clause here fails naming it: a wire name or a published
  function that binds no row, a row naming an action the policy lacks, an
  action no row names, a gated row that serves an unbound caller or reads its
  row before refusing, a wire verb without the options slot the credential
  rides.
  """
  # async: false: the auth stores are singletons and the posture is flipped.
  use FlezhaTest.Case, fixtures: [], async: false

  alias Flezha.AuthZ.{Action, Policy, Verb, Verbs}
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.{AuthTestHelper, RpcSurface}

  @published "docs/api/consumers/published.txt"
  @unbound %{source: :ui}
  @viewer %{source: :ui, principal: "vw@_system", role: :viewer, namespace: "_system"}

  test "every CliManager export binds a row" do
    bindings = Verb.bindings(CliManager)
    bound = MapSet.new(bindings, fn {name, arity, _verb, _opts} -> {name, arity} end)

    exports =
      CliManager.__info__(:functions)
      |> Enum.reject(&(&1 in [{:__verbs__, 0}, {:__verb_surface__, 0}]))
      |> Enum.reject(&MapSet.member?(bound, &1))

    assert exports == [], "wire names bound to no row: #{inspect(exports)}"

    for {_name, _arity, verb, _opts} <- bindings, do: assert(%{verb: ^verb} = Verbs.fetch!(verb))
  end

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

  test "every row names an action the policy knows, or is public or none" do
    strays =
      for row <- Verbs.all(),
          row.action not in [:public, :none],
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

        assert match?({:error, {:forbidden, _}}, result),
               "#{row.verb} served an unbound caller: #{inspect(result)}"

        refute_received {:acted, ^row}, "#{row.verb} ran its effect for an unbound caller"
        refute_received {:fetched, ^row}, "#{row.verb} read its row for an unbound caller"

        denials = AuthTestHelper.await_platform_authz_events(:authorization_denied, boundary, 4)
        assert length(denials) == 1, "#{row.verb}: #{length(denials)} denials audited"
      end
    end

    test "a row-scoped verb refuses a role the action is not granted to before it reads the row" do
      for row <- gated(),
          match?({:row, _}, row.scope),
          not Policy.grants?(:viewer, row.action) do
        assert {:error, {:forbidden, :role_insufficient}} = drive(row, @viewer)
        refute_received {:fetched, ^row}, "#{row.verb} read its row for a viewer"
        refute_received {:acted, ^row}
      end
    end
  end

  test "every wire verb ends with its options list" do
    rows = Map.new(Verbs.all(), &{&1.verb, &1})
    bindings = Verb.bindings(CliManager)

    missing =
      for {{:function, name, _arity}, _anno, [signature | _], _doc, _meta} <- docs(CliManager),
          {^name, _arity, verb, _opts} <-
            Enum.take(Enum.filter(bindings, &(elem(&1, 0) == name)), 1),
          rows[verb].action != :none,
          not String.ends_with?(signature, "opts \\\\ [])"),
          do: signature

    assert missing == [], "wire verbs without a trailing options list: #{inspect(missing)}"
  end

  defp gated, do: Enum.reject(Verbs.all(), &(&1.action in [:public, :none]))

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
    do: Enum.any?(Verb.bindings(module), &match?({^fun, ^arity, _verb, _opts}, &1))

  # Every documented public function whose last parameter is the caller
  # context, as its head spells it. A `@doc false` function is an effect the
  # terminal shares, recorded as such; the manifest test above keeps it off the
  # published surface.
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
