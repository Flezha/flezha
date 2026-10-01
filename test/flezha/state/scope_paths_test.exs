defmodule FlezhaTest.State.ScopePathsTest do
  @moduledoc """
  A stepped scope's state nests under its outer instance's directory and is
  found by its journal, so whatever moves, adopts or closes the instance's
  state carries its scopes.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  alias Flezha.Operator.{ClassRegistry, Scopes}
  alias Flezha.State.{Paths, ScopePaths, TestHelpers}

  setup do
    outer = "gateway_#{System.unique_integer([:positive])}"
    alice = ScopePaths.scope_id(outer, "session", "alice")
    odd = ScopePaths.scope_id(outer, "session", "a b/c")
    lone = ScopePaths.scope_id(outer, "audit", nil)

    for scope <- [alice, odd, lone] do
      TestHelpers.write_wal(Paths.journal_id(scope), 0..1, "journal")
      TestHelpers.write_wal({scope, "turn", "k"}, 0..1, "cell")
    end

    # A tasklet of the outer instance, whose directory is no scope.
    TestHelpers.write_wal({outer, "reply", "x"}, 0..1, "cell")

    # As the instance's bind declares the nodes that embed a class.
    embedding = %{"session" => "session", "audit" => "audit"}

    :ok =
      ClassRegistry.put_declarations(outer, %{
        ClassRegistry.decode_declarations("")
        | scopes: embedding
      })

    %{outer: outer, alice: alice, odd: odd, lone: lone}
  end

  # fails when: a key that is not a safe file name is read back as its encoding,
  # or an unkeyed edge's scope is missed.
  test "an instance's scopes are found by their journals, keyed and unkeyed", ctx do
    assert Enum.sort(ScopePaths.scopes_of(ctx.outer)) ==
             [{"audit", nil}, {"session", "a b/c"}, {"session", "alice"}]

    assert ScopePaths.split(ctx.odd) == {ctx.outer, {"session", "a b/c"}}
    assert ScopePaths.split(ctx.lone) == {ctx.outer, {"audit", nil}}
    assert ScopePaths.split(ctx.outer) == :error
  end

  # fails when: a frame for a scope names the scope's whole id, which a frame
  # header cuts at an operator id's length.
  test "a scope's frame is addressed to its instance", ctx do
    assert {outer, {"m", %{"flz.scope" => suffix, "channel" => "turns"}}} =
             Scopes.addressed(ctx.alice, {"m", %{"channel" => "turns"}})

    assert outer == ctx.outer
    assert outer <> suffix == ctx.alice
    assert Scopes.addressed(ctx.outer, {"m", %{}}) == {ctx.outer, {"m", %{}}}
  end

  # fails when: an adoption or a move of the instance leaves its scopes behind.
  test "an instance's durable refs carry its scopes'", ctx do
    refs = Paths.durable_refs(ctx.outer)

    for scope <- [ctx.alice, ctx.odd, ctx.lone] do
      assert {:cell, Paths.journal_id(scope)} in refs
      assert {:cell, {scope, "turn", "k"}} in refs
    end

    assert {:cell, {ctx.outer, "reply", "x"}} in refs
  end

  # fails when: a class whose bind declared no embedding still has its
  # directory walked for scopes on every move, stop and adoption, or one that
  # embeds loses its scopes once its registry row is gone.
  test "a class that embeds nothing is not searched for scopes", ctx do
    declared = fn scopes -> %{ClassRegistry.decode_declarations("") | scopes: scopes} end

    :ok = ClassRegistry.put_declarations(ctx.outer, declared.(%{}))
    assert ScopePaths.scope_ids(ctx.outer) == []
    refute {:cell, Paths.journal_id(ctx.alice)} in Paths.durable_refs(ctx.outer)

    :ok = ClassRegistry.put_declarations(ctx.outer, declared.(%{"session" => "session"}))
    assert ctx.alice in ScopePaths.scope_ids(ctx.outer)

    # A stopped class's state still moves with its scopes.
    ClassRegistry.delete(ctx.outer)
    assert ctx.alice in ScopePaths.scope_ids(ctx.outer)
  end

  # fails when: a scope's ref under a staged copy lands beside the instance
  # rather than in the staged tree, or does not read back as the live scope.
  test "a scope's ref under a staged copy is the same scope there", ctx do
    staged = Paths.staging_class(ctx.outer)

    {:cell, {staged_alice, "turn", "k"}} =
      Paths.ref_under({:cell, {ctx.alice, "turn", "k"}}, staged)

    assert staged_alice == staged <> "/session/alice"
    assert Paths.adopting_for(staged_alice) == ctx.alice
    assert Paths.ref_under({:input_log, staged_alice}, ctx.outer) == {:input_log, ctx.alice}
  end

  # fails when: a scope's writer can start while its instance's copy is switched.
  test "a scope's writer start waits for its instance's switch", ctx do
    cell = {ctx.alice, "turn", "k"}

    task =
      Flezha.State.Supervisor.without_new_writers(ctx.outer, fn ->
        task = Task.async(fn -> Flezha.State.Supervisor.find_or_start(cell) end)
        refute Task.yield(task, 200), "a scope's writer started inside its instance's switch"
        task
      end)

    assert {:ok, writer} = Task.await(task)

    # Closing the instance's writers closes its scopes'.
    ref = Process.monitor(writer)
    assert :ok = Flezha.State.Supervisor.close_writers(ctx.outer)
    assert_receive {:DOWN, ^ref, :process, ^writer, _reason}, 5_000
  end
end
