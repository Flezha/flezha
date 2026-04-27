defmodule FlezhaTest.ComputeEngine.OrphanFinderTest do
  alias Flezha.ComputeEngine.OrphanFinder

  use ExUnit.Case, async: true

  defp cspec(processor_id, factory_id, ns \\ "ns1", ce \\ "old_ce") do
    %{
      id: processor_id,
      start:
        {Flezha.Processor.Overwatch, :start_link,
         [
           {ns, ce,
            %Flezha.Processor.Builder{
              id: processor_id,
              factory_id: factory_id,
              init_values: %{},
              opts: []
            }}
         ]}
    }
  end

  defp entry(processor_id, factory_id, tag, node_pids) do
    {processor_id, {cspec(processor_id, factory_id), node_pids, %{tag: tag}}}
  end

  describe "collect/5" do
    setup do
      curr = node()
      peer = :"peer@example.test"
      pid_a = spawn(fn -> :timer.sleep(:infinity) end)
      pid_b = spawn(fn -> :timer.sleep(:infinity) end)
      pid_c = spawn(fn -> :timer.sleep(:infinity) end)

      on_exit(fn ->
        for p <- [pid_a, pid_b, pid_c], Process.alive?(p), do: Process.exit(p, :kill)
      end)

      %{curr: curr, peer: peer, pid_a: pid_a, pid_b: pid_b, pid_c: pid_c}
    end

    test "returns local processors tagged with a non-active ce whose factory matches", ctx do
      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", [{ctx.curr, ctx.pid_a}])
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(),
          MapSet.new(["new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == [{"p1", ctx.pid_a}]
    end

    test "skips processors already claimed by try_migrate_compute_engine", ctx do
      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", [{ctx.curr, ctx.pid_a}]),
          entry("p2", "f1", "dead_ce", [{ctx.curr, ctx.pid_b}])
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(["p1"]),
          MapSet.new(["new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == [{"p2", ctx.pid_b}]
    end

    test "skips processors with non-matching factory_id", ctx do
      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", [{ctx.curr, ctx.pid_a}]),
          entry("p2", "f_other", "dead_ce", [{ctx.curr, ctx.pid_b}])
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(),
          MapSet.new(["new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == [{"p1", ctx.pid_a}]
    end

    test "skips processors hosted on a peer node", ctx do
      registry =
        Map.new([
          entry("p1", "f1", "dead_ce", [{ctx.peer, ctx.pid_a}]),
          entry("p2", "f1", "dead_ce", [{ctx.curr, ctx.pid_b}])
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(),
          MapSet.new(["new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == [{"p2", ctx.pid_b}]
    end

    test "skips processors whose tag still points at an active ce", ctx do
      registry =
        Map.new([
          entry("p1", "f1", "active_ce_1", [{ctx.curr, ctx.pid_a}]),
          entry("p2", "f1", "dead_ce", [{ctx.curr, ctx.pid_b}])
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(),
          MapSet.new(["active_ce_1", "new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == [{"p2", ctx.pid_b}]
    end

    test "skips entries with no :tag in metadata (conservative)", ctx do
      registry =
        Map.new([
          {"p1",
           {cspec("p1", "f1"), [{ctx.curr, ctx.pid_a}], %{}}}
        ])

      result =
        OrphanFinder.collect(
          registry,
          ctx.curr,
          MapSet.new(),
          MapSet.new(["new_ce"]),
          MapSet.new(["f1"])
        )

      assert result == []
    end
  end

  describe "factory_ids_from/1" do
    test "extracts ids from a list of factory maps" do
      factories = [
        %{"id" => "f1", "description" => "x"},
        %{"id" => "f2", "description" => nil}
      ]

      assert OrphanFinder.factory_ids_from(factories) == MapSet.new(["f1", "f2"])
    end

    test "drops entries with nil id" do
      factories = [%{"id" => nil}, %{"id" => "f1"}]
      assert OrphanFinder.factory_ids_from(factories) == MapSet.new(["f1"])
    end
  end

  describe "factory_id_from_cspec/1" do
    test "extracts factory_id from a 3-tuple start arg" do
      assert OrphanFinder.factory_id_from_cspec(cspec("p1", "f1")) == "f1"
    end

    test "extracts factory_id from a 2-tuple start arg" do
      cspec_2tuple = %{
        start:
          {Flezha.Processor.Overwatch, :start_link,
           [{"ns1", %Flezha.Processor.Builder{id: "p1", factory_id: "f1"}}]}
      }

      assert OrphanFinder.factory_id_from_cspec(cspec_2tuple) == "f1"
    end

    test "returns nil when shape doesn't match" do
      assert OrphanFinder.factory_id_from_cspec(%{}) == nil
    end
  end
end
