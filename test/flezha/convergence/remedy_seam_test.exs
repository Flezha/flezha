defmodule Flezha.Convergence.RemedySeamTest do
  @moduledoc """
  The one seam every remedy passes: `Heal.execute/1`, planned by the Planner
  from a Diff that consumes every declared fact through one field.

  Five clauses, table-driven over `Heal.primitives/0`, the declared kinds and
  the discovered shutdown steps, so a new primitive, kind or step is judged
  with no test edit: every primitive has a key rule, an executor and an
  evidence rule and fails safely on a missing target; a ladder resets on
  evidence only; every kind is consumed by one Diff field and planned by one
  rule; no source outside `lib/flezha/convergence/` calls a remedy entry
  point; and every teardown step is discovered, ordered and reached.
  """

  # async: false: Mimic global mode; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias FlezhaTest.Helper.ConvergenceFixtures

  alias Flezha.Config.Backoff

  alias Flezha.Convergence.{
    Diff,
    Facts,
    Heal,
    Intent,
    Loop,
    Planner,
    Probe,
    RecoveryGate
  }

  alias Flezha.Identificator
  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @namespace "remedy_seam_test"
  @dead_runner "runner_that_disconnected"
  @live_runner "live_market_runner"

  # ---------- clause 1: every primitive has a key, an executor, an evidence rule ----------

  # One sample op per primitive, against a target that does not exist: what
  # the Planner would emit, and what the executor must answer without a crash.
  @ghost_ops %{
    rebind_operator: %{namespace: "ghost_ns", target: "ghost_op", args: %{}},
    resync_runner: %{namespace: "ghost_ns", target: "ghost_op", args: %{reason: :output_stall}},
    start_plugin: %{
      namespace: "ghost_ns",
      target: "ghost_op",
      args: %{
        plugin_id: "p",
        plugin: %Plugin{id: "p", provider: __MODULE__.NoopProvider, opts: %{}, hooks: []}
      }
    },
    stop_plugin: %{namespace: "ghost_ns", target: "ghost_op", args: %{plugin_id: "p"}},
    restart_plugin: %{
      namespace: "ghost_ns",
      target: "ghost_op",
      args: %{
        plugin_id: "p",
        plugin: %Plugin{id: "p", provider: __MODULE__.NoopProvider, opts: %{}, hooks: []}
      }
    },
    evict_runner: %{namespace: nil, target: "ghost_runner", args: %{}},
    restart_runner: %{namespace: nil, target: "ghost_runner", args: %{}},
    reset_control_channel: %{
      namespace: nil,
      target: "ghost_runner",
      args: %{operator_id: "ghost_op"}
    },
    publish_binding: %{
      namespace: "ghost_ns",
      target: "ghost_op",
      args: %{tag: "old", bound: "new"}
    }
  }

  # What a healthy answer to a missing target is: a failed heal, or an
  # idempotent `:ok` for the one primitive whose absence IS the goal.
  @healthy_answer %{evict_runner: :ok}

  test "clause 1: every primitive has a key rule, an executor and an evidence entry, and fails safely on a missing target" do
    for primitive <- Heal.primitives() do
      entry = Enum.find(Heal.table(), &(&1.name == primitive))
      assert entry, "primitive #{primitive} has no table entry"
      assert is_binary(entry.executor) and entry.executor != "", "#{primitive} names no executor"
      assert is_atom(entry.evidence), "#{primitive} names no evidence"

      sample = Map.fetch!(@ghost_ops, primitive)
      op = Map.put(sample, :primitive, primitive)

      assert {key_head, _key_target} = Planner.key(op)
      assert is_atom(key_head), "#{primitive} has no key rule"

      assert Heal.execute(op) == Map.get(@healthy_answer, primitive, :error),
             "#{primitive} did not answer a missing target safely"
    end
  end

  test "clause 1: the sample table covers exactly the primitive table" do
    assert Enum.sort(Map.keys(@ghost_ops)) == Enum.sort(Heal.primitives()),
           "a primitive was added or removed without a sample here"
  end

  # ---------- clause 2: a ladder resets on evidence only ----------

  test "clause 2: every evidence-reset kind is a ladder and every inline kind is not" do
    for {kind, entry} <- Facts.holding(),
        is_atom(entry[:reason]) and not is_nil(entry[:reason]) do
      op = %{primitive: :rebind_operator, target: "t", args: %{reason: entry.reason}}

      assert Planner.ladder?(op) == (entry.reset == :evidence),
             "kind #{kind} (reason #{entry.reason}) has reset #{entry.reset} but ladder? disagrees"

      assert Planner.key(op) == {entry.reason, "t"}
    end

    assert Facts.ladder_primitive?(:restart_plugin)
    refute Facts.ladder_primitive?(:start_plugin)
  end

  describe "clause 2: the loop keeps a ladder's key armed while the fact stands" do
    setup do
      # One declared operator, parked, on a live runner: the probe's truth for
      # every tick below. Every executor answers `:ok` without touching anything.
      intent = %Intent{
        operators: [%{namespace: :ns, operator_id: "op", factory_id: "f", plugins: []}]
      }

      stub(Intent, :declared, fn -> intent end)
      stub(RecoveryGate, :ready_namespaces, fn _ -> MapSet.new([:ns]) end)
      stub(Heal, :execute, fn _op -> :ok end)
      stub(HubManager, :durable_registry_pending?, fn -> false end)

      start_supervised!({Loop, tick_ms: 3_600_000})
      :ok
    end

    defp reality(observed) do
      ConvergenceFixtures.snapshot(
        %{
          {:ns, "op"} => Map.merge(%{pid: self(), bound_runner: "e1", factory_id: "f"}, observed)
        },
        %{ns: MapSet.new(["e1"])}
      )
    end

    test "a stub executor answering :ok leaves a parked ladder armed until the park is observed gone" do
      stub(Probe, :snapshot, fn ->
        reality(%{parked?: true, park_reason: {:ping_failed, :timeout}})
      end)

      :ok = Loop.tick_now()

      backoff = :sys.get_state(Loop).backoff
      assert Backoff.attempt(backoff, {:parked, "op"}) == 1, "the parked rung did not advance"
      assert Map.has_key?(:sys.get_state(Loop).facts, {:operator_parked, {:ns, "op"}})

      # Repaired: the probe sees it unparked, the fact goes and the key resets.
      stub(Probe, :snapshot, fn -> reality(%{parked?: false, park_reason: nil}) end)
      :ok = Loop.tick_now()

      assert Backoff.attempt(:sys.get_state(Loop).backoff, {:parked, "op"}) == 0
      refute Map.has_key?(:sys.get_state(Loop).facts, {:operator_parked, {:ns, "op"}})
    end

    test "an inline op resets its key on its own :ok" do
      stub(Probe, :snapshot, fn -> reality(%{bound_runner: "e_dead"}) end)
      Facts.declare({:runner_disconnected, {:ns, "e_dead"}, %{}})
      :ok = Loop.tick_now()

      assert Backoff.attempt(:sys.get_state(Loop).backoff, {:runner_disconnected, "op"}) == 0
    end
  end

  # ---------- clause 3: every kind is consumed by one field and planned by one rule ----------

  # One populated Diff per rule, with the primitives it must plan, so a rule
  # is checked by what it plans rather than by what it says.
  @plugin %Plugin{
    id: "p1",
    provider: __MODULE__.NoopProvider,
    opts: %{},
    hooks: [],
    namespace: "ns",
    operator_id: "r"
  }
  @rule_samples %{
    misbound_operators: %Diff{misbound_operators: [{{:ns, "r"}, "e_dead", :probe}]},
    parked_operators: %Diff{
      parked_operators: [
        {{:ns, "r"}, %{reason: {:ping_failed, :closed}, transport: :dead}, "e1"}
      ]
    },
    data_plane_wedge: %Diff{
      data_plane_wedge: {:runner, :ns, "e1"},
      data_plane_layer: :runner_pool_wedged
    },
    unbound_operators: %Diff{unbound_operators: [{:ns, "r"}]},
    resync_cells: %Diff{resync_cells: [{{"c", "p", "k"}, "e1", :seq_gap}]},
    missing_plugins: %Diff{missing_plugins: [{{:ns, "r"}, "p1", @plugin}]},
    extra_plugins: %Diff{extra_plugins: [{{:ns, "r"}, "p1", nil}]},
    unhealthy_plugins: %Diff{unhealthy_plugins: [{{:ns, "r"}, "p1", @plugin, :owned_pid_dead}]},
    stale_tags: %Diff{stale_tags: [{{:ns, "r"}, "e_old", "e1"}]},
    stalled_operators: %Diff{stalled_operators: [{:ns, "r"}]}
  }

  test "clause 3: every declared kind is consumed by exactly one Diff field and planned by exactly one rule" do
    fields = Map.keys(%Diff{}) -- [:__struct__]
    rules = Planner.rules()

    for {kind, entry} <- Facts.holding() do
      assert entry.diff in fields,
             "kind #{kind} names Diff field #{entry.diff}, which does not exist"

      case entry.diff do
        # The drain's suppression: consumed, never planned.
        :draining ->
          refute Keyword.has_key?(rules, :draining)

        field ->
          assert Enum.count(rules, fn {f, _} -> f == field end) == 1,
                 "kind #{kind}'s field #{field} is planned by #{Enum.count(rules, fn {f, _} -> f == field end)} rules, not one"
      end
    end
  end

  test "clause 3: every primitive is named by a rule, every rule plans only primitives, and plans what it says" do
    rules = Planner.rules()
    named = rules |> Keyword.values() |> List.flatten() |> Enum.uniq()

    for primitive <- Heal.primitives() do
      assert primitive in named, "primitive #{primitive} is planned by no rule"
    end

    for {field, primitives} <- rules do
      for p <- primitives,
          do: assert(p in Heal.primitives(), "rule #{field} plans unknown primitive #{p}")

      sample = Map.fetch!(@rule_samples, field)

      planned =
        sample |> Planner.plan(Backoff.new(:heal)) |> Enum.map(& &1.primitive) |> Enum.uniq()

      assert planned != [] and Enum.all?(planned, &(&1 in primitives)),
             "rule #{field} planned #{inspect(planned)}, declared #{inspect(primitives)}"
    end

    assert Enum.sort(Map.keys(@rule_samples)) == Enum.sort(Keyword.keys(rules)),
           "a rule was added or removed without a sample here"
  end

  test "clause 3: a re-declaration of a held key refreshes it and costs no tick; a new key ticks" do
    now = 1_000
    {facts, :new} = Facts.put(%{}, :output_stall, "op", %{}, now)
    assert Facts.ticks?(:output_stall, :new)

    {facts, :known} = Facts.put(facts, :output_stall, "op", %{}, now + 5)
    refute Facts.ticks?(:output_stall, :known)
    assert facts[{:output_stall, "op"}].at == now + 5

    {facts, :cleared} = Facts.put(facts, :seq_gap_closed, {"c", "p", "k"}, %{}, now)
    refute Facts.ticks?(:seq_gap_closed, :cleared)
    {_facts, :transient} = Facts.put(facts, :runner_connected, {:ns, "e1"}, %{}, now)
    assert Facts.ticks?(:runner_connected, :transient)
  end

  # ---------- clause 4: a remedy entry point outside the loop is a bypass ----------

  # The functions only `lib/flezha/convergence/` may call. `drain_hub/2` is
  # the shutdown plan's alone; the takeover evict and `runner_preserve` are
  # not remedies (the connect protocol's answer to the runner's own
  # declaration, and a keepalive fact) and keep their files; the restart
  # executor's body lives in `Plugins.restart/3`.
  @entry_points [
    {Flezha.State.Recovery, :resync, []},
    {Flezha.State.Recovery, :resync_for_overwatch, []},
    {Flezha.Convergence.Heal, :any, []},
    {Flezha.Runner.FleetManager, :evict_runner, []},
    {Flezha.Runner.FleetManager, :reset_control_channel, []},
    {Flezha.Operator.Overwatch, :plugin_restart, []},
    {Flezha.Plugin.Extension.Table, :attempt_restart, ["lib/flezha/operator/plugins.ex"]},
    # A bind publishes its OWN binding, which is not a remedy: those sites run
    # in the Overwatch as it binds, and the third IS the primitive's executor
    # (the `:publish_binding` call the loop makes). Repairing a row that named
    # someone else is the loop's, and it goes through that call.
    {Flezha.Operator.Lifecycle, :publish_binding,
     [
       "lib/flezha/operator/lifecycle.ex",
       "lib/flezha/operator/migration.ex",
       "lib/flezha/operator/overwatch.ex"
     ]},
    {Flezha.Runner.ControlUnit, {:disconnect, :terminal}, []},
    {Flezha.Service.HubManager, :drain_hub,
     ["lib/flezha/shutdown/", "lib/flezha/service/hub_manager/lifecycle.ex"]},
    {Flezha.Service.HubManager.Lifecycle, :drain_hub,
     ["lib/flezha/shutdown/", "lib/flezha/service/hub_manager/lifecycle.ex"]}
  ]

  test "clause 4: no source outside lib/flezha/convergence/ calls a remedy entry point" do
    bypasses =
      for {file, calls} <- remote_calls("lib/**/*.ex"),
          not String.starts_with?(file, "lib/flezha/convergence/"),
          {module, fun, line, args} <- calls,
          {entry_mod, entry_fun, allowed} <- @entry_points,
          module == entry_mod,
          entry_point?(entry_fun, fun, args),
          not Enum.any?(allowed, &String.starts_with?(file, &1)),
          do: "#{file}:#{line} #{inspect(module)}.#{fun}/#{length(args)}"

    assert bypasses == [],
           "remedy entry points called outside the loop:\n  " <> Enum.join(bypasses, "\n  ")
  end

  # `migrate_child/3` is not a remedy any more, and no longer a remedy anyone
  # may bypass the loop with: the hub decides where a child belongs, and Flezha
  # never names a destination. `drain/2`, which the graceful shutdown reaches
  # through its one seam, is the only migration call left in the tree.
  test "clause 4: no source in lib/ moves a child between nodes itself" do
    movers =
      for {file, calls} <- remote_calls("lib/**/*.ex"),
          {ProcessHub.Service.Migration, :migrate_child, line, _args} <- calls,
          do: "#{file}:#{line}"

    assert movers == [],
           "lib/ names a destination node for a child:\n  " <> Enum.join(movers, "\n  ")
  end

  defp entry_point?(:any, _fun, _args), do: true
  defp entry_point?({:disconnect, :terminal}, :disconnect, args), do: terminal?(args)
  defp entry_point?(entry_fun, fun, _args), do: entry_fun == fun

  defp terminal?(args) do
    Enum.any?(args, fn
      opts when is_list(opts) -> Enum.any?(opts, &match?({:terminal, true}, &1))
      _ -> false
    end)
  end

  # ---------- clause 5: every teardown step is discovered, ordered and reached ----------

  test "clause 5: every step is discovered with a unique order within its scope" do
    steps = Shutdown.all_steps()
    assert steps != []

    for scope <- [:process, :operator, :namespace, :node] do
      orders = steps |> Enum.filter(&(&1.scope == scope)) |> Enum.map(& &1.order)
      assert orders != [], "scope #{scope} declares no step"
      assert orders == Enum.uniq(orders), "scope #{scope} has a duplicate order"
      assert orders == Enum.sort(orders)
    end

    for step <- steps do
      assert is_function(step.run, 2), "step #{step.scope}/#{step.name} has no run/2"
      assert Code.ensure_loaded?(step.module)
    end
  end

  @stop_entry_points [
    {"lib/flezha/operator/migration.ex", :process},
    {"lib/flezha/service/operator_manager.ex", :operator},
    {"lib/flezha/runner/fleet_manager.ex", :namespace},
    {"lib/flezha/shutdown/coordinator.ex", :node}
  ]

  test "clause 5: the four stop entry points reach Shutdown.run/3" do
    calls = remote_calls("lib/**/*.ex") |> Map.new()

    for {file, scope} <- @stop_entry_points do
      walks =
        for {Flezha.Shutdown, :run, _line, [first | _]} <- Map.get(calls, file, []),
            first == scope,
            do: file

      assert walks != [], "#{file} does not call Shutdown.run(#{inspect(scope)}, …)"
    end
  end

  # A plugin table is torn down by the process step alone, a handover
  # rebuilds one (not a teardown), and a shared plugin's copy tears down the
  # table it owns when it stops or restarts. Durable state is deleted through the one
  # guard in `Paths`; the hub registry directory is the hub step's own.
  #
  # Scoped to the controller tree: `lib/mix/` is the bench harness, which
  # clears its own namespace directory BEFORE the application boots — offline,
  # like `flz reset`, and never a teardown of a running operator. The plugin
  # installer and loader delete only their own expansions of package archives.
  @teardown_allowed %{
    {Flezha.Plugin.Extension.Table, :teardown_all} => [
      "lib/flezha/operator/migration.ex",
      "lib/flezha/operator/handover.ex",
      "lib/flezha/plugin/shared.ex"
    ],
    {File, :rm_rf} => [
      "lib/flezha/state/paths.ex",
      "lib/flezha/service/hub_manager/lifecycle.ex",
      "lib/flezha/plugin/installer.ex",
      "lib/flezha/transport/socket/support.ex"
    ],
    {File, :rm_rf!} => [
      "lib/flezha/plugin/installer.ex",
      "lib/flezha/plugin/installer/loader.ex"
    ]
  }

  test "clause 5: a teardown outside the steps fails naming the module, function and arity" do
    offenders =
      for {file, calls} <- remote_calls("lib/flezha/**/*.ex"),
          {module, fun, line, args} <- calls,
          {{entry_mod, entry_fun}, allowed} <- @teardown_allowed,
          module == entry_mod and fun == entry_fun,
          file not in allowed,
          do: "#{file}:#{line} #{inspect(module)}.#{fun}/#{length(args)}"

    assert offenders == [], "teardown outside the steps:\n  " <> Enum.join(offenders, "\n  ")
  end

  # ---------- the executor contract, driven through Heal.execute/1 ----------

  describe "the rebind executor" do
    # An Overwatch stand-in: replies the full struct — as the heal's snapshot
    # and consent reads require — with a scriptable `{:runner_migrate, _}` reply.
    defmodule FakeOverwatch do
      use GenServer

      def start_link(ow, migrate \\ :confirm), do: GenServer.start_link(__MODULE__, {ow, migrate})

      @impl true
      def init(state), do: {:ok, state}

      @impl true
      def handle_call(:get_overwatch_state, _from, {ow, _migrate} = state),
        do: {:reply, ow, state}

      def handle_call({:runner_migrate, _target}, _from, {_ow, :confirm} = state),
        do: {:reply, :ok, state}

      def handle_call({:runner_migrate, target}, _from, {_ow, {:confirm, test_pid}} = state) do
        send(test_pid, {:runner_migrate, target})
        {:reply, :ok, state}
      end

      def handle_call({:runner_migrate, _target}, _from, {_ow, :refuse} = state),
        do: {:reply, {:error, :busy}, state}

      # Never replies: the caller's bounded call must time out (~2 s).
      def handle_call({:runner_migrate, _target}, _from, {_ow, :hang} = state),
        do: {:noreply, state}
    end

    setup do
      fleet_state = :sys.get_state(FleetManager)
      on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet_state end) end)

      {:ok, _} = HubRegistryTestHelper.ensure_hub(@namespace)
      on_exit(fn -> HubManager.stop_hub(@namespace) end)

      HubRegistryTestHelper.seed_runner(@namespace, @live_runner, "market")
      :ok
    end

    # The Overwatch publishes its own binding once the start answers, so what
    # the executor decides is the runner it asks the Overwatch to move to.
    test "a confirmed migrate asks the Overwatch to move to the factory-filtered pick" do
      id = "confirming_op"
      inject(id, factory: "market", migrate: {:confirm, self()})

      assert Heal.execute(rebind(id)) == :ok
      assert_received {:runner_migrate, @live_runner}
    end

    test "a timed-out, refused or unplaceable migrate leaves the registry binding unchanged" do
      # The third row differs in its outcome and not in its effect: no runner
      # advertises that factory, so the rebind never had a repair to attempt
      # and answers the unmet precondition the loop charges no rung for.
      for {id, outcome, opts} <- [
            {"hanging_op", :error, [factory: "market", migrate: :hang]},
            {"refusing_op", :error, [factory: "market", migrate: :refuse]},
            {"unservable_op", :unmet, [factory: "factory_nobody_serves", migrate: :confirm]}
          ] do
        inject(id, opts)
        assert Heal.execute(rebind(id)) == outcome
        assert registry_tag(id) == @dead_runner
      end
    end

    defp rebind(id),
      do: %{primitive: :rebind_operator, namespace: @namespace, target: id, args: %{}}

    defp inject(id, opts) do
      {:ok, pid} =
        FakeOverwatch.start_link(
          %Overwatch{
            id: id,
            namespace: @namespace,
            factory_id: Keyword.fetch!(opts, :factory),
            runner_id: @dead_runner
          },
          Keyword.fetch!(opts, :migrate)
        )

      cspec = %{
        id: id,
        start:
          {HubRegistryTestHelper.StubChild, :start_link,
           [{@namespace, @dead_runner, %{id: id, factory_id: Keyword.fetch!(opts, :factory)}}]}
      }

      hub_id = Identificator.hub(@namespace)

      :ok =
        ProcessRegistry.bulk_insert(hub_id, %{
          id => {cspec, [{node(), pid}], %{tag: @dead_runner}}
        })
    end

    defp registry_tag(id) do
      hub_id = Identificator.hub(@namespace)
      {_cspec, _node_pids, metadata} = Map.fetch!(ProcessRegistry.dump(hub_id), id)
      metadata[:tag]
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end

  # ---------- the source walk ----------

  # Every remote call in every file under `glob`, as `{file, [{module, fun,
  # line, args}]}`, with the file's aliases resolved: `alias A.B`, `alias
  # A.{B, C}` and `alias A.B, as: C` — the three spellings the tree uses.
  defp remote_calls(glob) do
    for file <- Path.wildcard(glob), into: %{} do
      {:ok, ast} = Code.string_to_quoted(File.read!(file), file: file, columns: false)
      aliases = aliases_of(ast)

      {_, calls} =
        Macro.prewalk(ast, [], fn
          {{:., meta, [{:__aliases__, _, parts}, fun]}, _, args} = node, acc
          when is_atom(fun) and is_list(args) ->
            {node, [{resolve(aliases, parts), fun, Keyword.get(meta, :line), args} | acc]}

          node, acc ->
            {node, acc}
        end)

      {file, Enum.reverse(calls)}
    end
  end

  defp aliases_of(ast) do
    {_, aliases} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, [{:__aliases__, _, parts}]} = node, acc ->
          {node, Map.put(acc, List.last(parts), Module.concat(parts))}

        {:alias, _, [{:__aliases__, _, parts}, [as: {:__aliases__, _, [as]}]]} = node, acc ->
          {node, Map.put(acc, as, Module.concat(parts))}

        {:alias, _, [{{:., _, [{:__aliases__, _, prefix}, :{}]}, _, children}]} = node, acc ->
          {node,
           Enum.reduce(children, acc, fn {:__aliases__, _, parts}, a ->
             Map.put(a, List.last(parts), Module.concat(prefix ++ parts))
           end)}

        node, acc ->
          {node, acc}
      end)

    aliases
  end

  defp resolve(aliases, [head | rest]) do
    case Map.get(aliases, head) do
      nil -> Module.concat([head | rest])
      module -> Module.concat([module | rest])
    end
  end
end
