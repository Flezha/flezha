defmodule Flezha.Api.ServiceSurfaceTest do
  @moduledoc """
  Every function on the published consumer surface, called once with a context
  that is not one.

  The requirement is that every published function except the two identity reads
  takes a `Flezha.Service.CallerContext` as its last argument and authorizes
  through it. A structural check cannot show that — a function can take the
  argument and ignore it — so each is called here with `%{}` and must refuse. A
  function that answered data instead would prove it never looked.

  The table is also the surface's call shapes written down: what each published
  function wants, in order, before the context.
  """
  use FlezhaTest.Case, fixtures: [], async: false

  alias Flezha.AuthZ.{Verb, Verbs}
  alias Flezha.Service.Controller
  alias Flezha.Service.Dlq
  alias Flezha.Service.Fleet
  alias Flezha.Service.Namespace
  alias Flezha.Service.Operator
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Schedule
  alias Flezha.Service.Bounded
  alias FlezhaTest.Helper.RpcSurface

  @published %{
    Controller => [
      {:hub_id, ["default"]},
      {:recovery_state, [:flezha_hub_default]},
      {:recovery_per_node, ["default", []]},
      {:shutdown_status, []},
      {:durability_backend, []},
      {:durability_posture, []},
      {:durability_breaches, []},
      {:durability_status, []},
      {:liveness_counters, []}
    ],
    Dlq => [
      {:list, [nil]},
      {:get, ["rc/task/id"]},
      {:stats, []},
      {:tasklets, [nil]},
      {:drop, ["rc/task/id"]},
      {:replay, ["rc/task/id"]}
    ],
    Fleet => [
      {:runners, ["default"]},
      {:runners_on_this_node, ["default"]},
      {:runners_for_factory, ["default", "factory"]},
      {:factories, ["default"]},
      {:runner_caller_context, ["default"]},
      {:hub_nodes, ["default"]},
      {:installed_plugins, []}
    ],
    Namespace => [
      {:list, []},
      {:get, ["default"]},
      {:quota_kinds, []},
      {:quota, ["default", :operators]},
      {:roles, ["default"]},
      {:create, ["ns_surface", []]},
      {:delete, ["ns_surface"]},
      {:set_quotas, ["default", %{}]}
    ],
    Operator => [
      {:list_namespaces, []},
      {:list_operators, ["default"]},
      {:operator_rows, ["default"]},
      {:operator_plugins, ["default"]},
      {:registry, ["default"]},
      {:partition_coverage, [[]]},
      {:partition_groups, [[]]},
      {:kind_known?, [:unbounded]},
      {:cell_state, ["default", "rc", "key"]},
      {:overwatch_state, ["default", "rc"]},
      {:start, ["default", %{}]},
      {:stop, ["default", "rc"]},
      {:send, ["default", "rc", "payload", ""]}
    ],
    PluginManager => [
      {:list, []},
      {:enable, ["plug_1", [id: "plug_1", provider: Flezha.Plugin]]},
      {:disable, ["plug_1"]}
    ],
    Schedule => [
      {:list, []},
      {:add, [%{name: "s"}]},
      {:remove, ["s"]},
      {:statuses, []},
      {:assert_entry, [%{name: "s"}]},
      {:retract, ["s"]},
      {:parse_cron, ["* * * * *"]},
      {:next_fire, [%{}, "Etc/UTC", ~U[2026-01-01 00:00:00Z]]}
    ],
    Bounded => [
      {:operator_classes, []},
      {:completion, ["instance"]},
      {:completed_retention_hours, ["rc", %{}]},
      {:journal, ["instance"]},
      {:committed_iterations, ["instance"]},
      {:consumed_offset, ["instance"]},
      {:undelivered, ["instance"]},
      {:last_offset, ["instance"]},
      {:state_metrics, []},
      {:rescale, ["default", "group", 2, []]},
      {:rewind, ["default", "rc", 1]},
      {:time_travel, ["cell", :latest]}
    ]
  }

  describe "every published function" do
    test "refuses a caller whose context is not one" do
      for {module, functions} <- @published, {fun, args} <- functions do
        assert match?(
                 {:error, %Flezha.Error{code: "authz.invalid_caller_context"}},
                 apply(module, fun, args ++ [%{}])
               ),
               "#{inspect(module)}.#{fun}/#{length(args) + 1} did not refuse an invalid context"
      end
    end

    test "refuses an invalid context on the identity-adjacent shapes too" do
      for {module, functions} <- @published, {fun, args} <- functions do
        assert match?(
                 {:error, %Flezha.Error{code: "authz.invalid_caller_context"}},
                 apply(module, fun, args ++ [%{source: :not_a_source}])
               ),
               "#{inspect(module)}.#{fun}/#{length(args) + 1} accepted an unknown :source"
      end
    end

    test "the table covers the whole published surface", %{} do
      recorded =
        for {module, functions} <- @published,
            {fun, args} <- functions,
            into: MapSet.new(),
            do: {module, fun, length(args) + 1}

      surface =
        "docs/api/consumers/published.txt"
        |> RpcSurface.manifest()
        |> Enum.map(fn {module, fun, arity, _where} -> {module, fun, arity} end)
        |> Enum.reject(fn {module, _fun, _arity} ->
          # The two domains that had a service API before this surface existed
          # keep their own suites, which drive them against live sinks.
          module in [Flezha.Service.AuditLog, Flezha.Service.Observability]
        end)
        |> MapSet.new()

      missing = MapSet.difference(surface, recorded)
      extra = MapSet.difference(recorded, surface)

      assert MapSet.to_list(missing) == [],
             "published but not exercised here: #{inspect(MapSet.to_list(missing))}"

      assert MapSet.to_list(extra) == [],
             "exercised here but not published: #{inspect(MapSet.to_list(extra))}"
    end
  end

  describe "an unbound caller under enforcement" do
    setup do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true}])
      :ok
    end

    # Which functions must refuse is the declaration's answer, not this file's:
    # every call shape above whose binding names a gated row.
    test "is refused by every function that binds a gated row" do
      assert Flezha.AuthZ.enforcement_posture() == :enforced
      rows = Map.new(Verbs.all(), &{&1.verb, &1})

      gated =
        for {module, functions} <- @published,
            {fun, args} <- functions,
            {^fun, arity, verb, _opts} <- Verb.bindings(module),
            arity == length(args) + 1,
            rows[verb].action not in [:public, :none],
            do: {module, fun, args}

      assert length(gated) > 40

      for {module, fun, args} <- gated do
        assert match?(
                 {:error, %Flezha.Error{class: :denied}},
                 apply(module, fun, args ++ [%{source: :ui}])
               ),
               "#{inspect(module)}.#{fun}/#{length(args) + 1} served an unbound caller under enforcement"
      end
    end
  end

  describe "the reads answer data for a valid caller" do
    test "the ones that need no fleet, hub or store running" do
      context = %{source: :ui, principal: "console@_system"}

      assert {:ok, kinds} = Namespace.quota_kinds(context)
      assert is_list(kinds) and kinds != []

      assert {:ok, true} = Operator.kind_known?(:unbounded, context)

      assert {:ok, []} = Operator.partition_groups([], context)
      assert {:ok, _coverage} = Operator.partition_coverage([], context)

      assert {:ok, hub} = Controller.hub_id("default", context)
      assert is_atom(hub)

      assert {:ok, posture} = Controller.durability_posture(context)
      assert posture in [:remote_tier, :local_only]

      assert {:ok, counters} = Controller.liveness_counters(context)
      assert is_map(counters) or is_list(counters)

      assert {:ok, classes} = Bounded.operator_classes(context)
      assert is_list(classes)

      assert {:ok, hours} = Bounded.completed_retention_hours("rc", %{}, context)
      assert is_integer(hours) and hours > 0

      assert {:ok, cron} = Schedule.parse_cron("* * * * *", context)
      assert {:ok, _next} = Schedule.next_fire(cron, "Etc/UTC", DateTime.utc_now(), context)

      assert {:error, _reason} = Schedule.parse_cron("not a cron", context)
    end
  end
end
