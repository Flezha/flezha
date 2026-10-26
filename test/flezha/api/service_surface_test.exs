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

  alias Flezha.AuthZ.Verbs
  alias Flezha.Service.Controller
  alias Flezha.Service.Dlq
  alias Flezha.Service.Fleet
  alias Flezha.Service.Namespace
  alias Flezha.Service.Relay
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Schedule
  alias Flezha.Service.Steps
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RpcSurface

  # The two domains that had a service API before this surface existed keep
  # their own suites, which drive them against live sinks.
  @own_suites [Flezha.Service.AuditLog, Flezha.Service.Observability]

  @published %{
    Controller => [
      {:durability_breaches, []},
      {:durability_status, []},
      {:liveness_counters, []},
      {:status, []},
      {:mem_top, []},
      {:boot_status, []},
      {:boot_await, [10]},
      {:cluster_status, ["default"]},
      {:stop, [[]]}
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
      {:runners_for_factory, ["default", "factory"]},
      {:factories, ["default"]},
      {:runner_caller_context, ["default"]},
      {:drop_runner, ["default", "runner"]},
      {:readmit_runner, ["default", "stable"]},
      {:dropped_runners, ["default"]},
      {:installed_plugins, []},
      {:providers, []},
      {:provider, ["relay_stdout"]}
    ],
    Namespace => [
      {:list, []},
      {:get, ["default"]},
      {:quota_kinds, []},
      {:quota, ["default", :relays]},
      {:roles, ["default"]},
      {:create, ["ns_surface", []]},
      {:delete, ["ns_surface"]},
      {:set_quotas, ["default", %{}]},
      {:set_migration, ["default", :cold]},
      {:whoami, []},
      {:bootstrap, ["token"]},
      {:issue_token, ["p", "default", "admin", []]},
      {:tokens, []},
      {:revoke_token, ["id"]},
      {:bind_role, ["p", "admin", "default"]},
      {:unbind_role, ["p", nil]},
      {:list_roles, []}
    ],
    Relay => [
      {:list_namespaces, []},
      {:relay_rows, ["default"]},
      {:relay_plugins, ["default"]},
      {:registry, ["default"]},
      {:partition_groups, [[]]},
      {:cell_state, ["default", "rc", "key"]},
      {:overwatch_state, ["default", "rc"]},
      {:start, ["default", %{}]},
      {:stop, ["default", "rc"]},
      {:call, ["default", "rc", "payload", "", [deadline_ms: 100]]},
      {:send, ["default", "rc", "payload", "", []]},
      {:list, ["default"]},
      {:info, ["default"]},
      {:listen, ["default", "rc", :output, []]},
      {:api, ["default", "rc"]},
      {:call_channel, ["default", "rc", "channel", "value", []]},
      {:diagnose, ["default", "rc"]},
      {:graph, ["default", "rc"]},
      {:activity, ["default", "rc", []]},
      {:replay, ["default", "path", "0"]}
    ],
    PluginManager => [
      {:enable, ["plug_1", [provider: Flezha.Plugin]]},
      {:bind, ["plug_1", [relay_id: "rc"]]},
      {:unbind, ["plug_1", [relay_id: "rc"]]},
      {:disable, ["plug_1", []]},
      {:enabled, ["default"]},
      {:update, ["plug_1", []]}
    ],
    Schedule => [
      {:list, []},
      {:add, [%{name: "s"}]},
      {:remove, ["s"]},
      {:run, ["s"]}
    ],
    Steps => [
      {:completion, ["instance"]},
      {:completed_retention_hours, ["rc", %{}]},
      {:journal, ["instance"]},
      {:committed_iterations, ["instance"]},
      {:consumed_offset, ["instance"]},
      {:undelivered, ["instance"]},
      {:last_offset, ["instance"]},
      {:rescale, ["default", "group", 2, []]},
      {:rewind, ["default", "rc", 1]},
      {:time_travel, ["cell", :latest]},
      {:state_get, ["rc/task/key"]},
      {:cells, ["rc"]},
      {:delete, ["rc/task/key"]},
      {:rotate, ["rc/task/key"]},
      {:set_retention, ["rc", 2]},
      {:outbox, ["rc/task/key"]},
      {:inbox, ["instance"]}
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
        "docs/internal/api/consumers/published.txt"
        |> RpcSurface.manifest()
        |> Enum.map(fn {module, fun, arity, _where} -> {module, fun, arity} end)
        |> Enum.reject(fn {module, _fun, _arity} -> module in @own_suites end)
        |> MapSet.new()

      assert Enum.sort(Map.keys(@published)) ==
               Enum.sort(Flezha.Service.modules() -- @own_suites)

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
            {^fun, arity, verb, _opts} <- module.__verbs__(),
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

      assert {:ok, []} = Relay.partition_groups([], context)

      assert {:ok, counters} = Controller.liveness_counters(context)
      assert is_map(counters) or is_list(counters)

      assert {:ok, hours} = Steps.completed_retention_hours("rc", %{}, context)
      assert is_integer(hours) and hours > 0
    end

    # A consumer on another node ages a signal against the controller's clock,
    # and reads the two fleet answers as `{:ok, value}` like every other read.
    test "the status carries the clock, and the fleet reads are wrapped once" do
      context = %{source: :ui, principal: "console@_system"}

      before = System.monotonic_time(:microsecond)
      assert {:ok, %{monotonic_us: clock}} = Controller.status(context)
      assert clock >= before and clock <= System.monotonic_time(:microsecond)

      assert {:ok, factories} = Fleet.factories("default", context)
      assert is_list(factories)

      assert {:ok, %{principal: "runner@default", namespace: "default"}} =
               Fleet.runner_caller_context("default", context)
    end

    # A console is pinned to one controller, and a relay runs on whichever one
    # placed it: read from the peer, a relay counted here must still show.
    test "the liveness read includes a relay held by another controller" do
      relay = "relay_#{System.unique_integer([:positive])}"
      LivenessCounters.bump_relay(LivenessCounters.register_relay(relay), :io_in_received)
      {_peer_pid, peer} = NodeTestHelper.start_bare_peer(:liveness_peer)

      assert %{relays: relays} = :erpc.call(peer, LivenessCounters, :snapshot_cluster, [])
      assert %{io_in_received: 1} = Enum.find(relays, &(&1.relay_id == relay))
    end
  end
end
