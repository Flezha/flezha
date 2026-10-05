defmodule FlezhaTest.Service.RelayManagerSpawnTest do
  @moduledoc """
  `RelayManager.spawn_or_deliver/6` — the controller half of a tasklet's
  `ctx.spawn_steps` (`tasklet-requested-spawn`).

  The registry is `FakeStartRegistry`, whose `insert_new` is the same atomic
  claim ProcessHub's `start_child` performs, so the concurrency case exercises
  the real race resolution rather than a serialized stand-in. Authorization is
  off here, so the caller context is judged and passes; the judgment itself is
  `FlezhaTest.Runner.ControlChannelSpawnAuthzTest`.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.Quotas
  alias Flezha.Cluster.Directory
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Placement
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.FakeStartRegistry
  alias FlezhaTest.Helper.FakeStartRegistry.Instance

  @namespace "spawn-or-deliver-test"
  @factory "worker"
  @caller %{source: :sdk, principal: "runner@" <> @namespace, namespace: @namespace}

  setup :set_mimic_global_until_exit

  setup do
    %{registry: FakeStartRegistry.install(runners: ["runner-1"], run: :steps)}
  end

  # The one request every case sends: instance "w1" in this module's namespace.
  defp spawn_or_deliver(factory, payload, if_running),
    do: RelayManager.spawn_or_deliver(@namespace, "w1", factory, payload, if_running, @caller)

  defp instance_pid(registry, id) do
    {:ok, pid} = FakeStartRegistry.pid_of(registry, id)
    pid
  end

  test "a free id spawns, carrying the payload as the instance's seed input" do
    assert {:spawned, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert [relay] = FakeStartRegistry.started()
    assert relay.run == :steps
    assert relay.factory_id == @factory
    assert Keyword.get(relay.opts, :seed_input) == {"p1", %{}}
  end

  test "a taken id receives the payload as one more input", %{registry: registry} do
    assert {:spawned, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert {:delivered, "w1"} =
             spawn_or_deliver(@factory, "p2", :deliver)

    assert Instance.ingested(instance_pid(registry, "w1")) == ["p2"]
  end

  test "the error policy reports instead of delivering", %{registry: registry} do
    assert {:spawned, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert {{:refused, %Flezha.Error{code: "relay.already_running"}}, "w1"} =
             spawn_or_deliver(@factory, "p2", :error)

    assert Instance.ingested(instance_pid(registry, "w1")) == []
  end

  test "a concurrent burst yields one instance and delivers every other payload", %{
    registry: registry
  } do
    outcomes =
      1..8
      |> Task.async_stream(
        fn n ->
          spawn_or_deliver(@factory, "p#{n}", :deliver)
        end,
        max_concurrency: 8,
        timeout: 10_000
      )
      |> Enum.map(fn {:ok, outcome} -> outcome end)

    assert Enum.count(outcomes, &(&1 == {:spawned, "w1"})) == 1
    assert Enum.count(outcomes, &(&1 == {:delivered, "w1"})) == 7

    delivered = Instance.ingested(instance_pid(registry, "w1"))
    assert length(delivered) == 7
    assert length(Enum.uniq(delivered)) == 7
  end

  test "an id freed by teardown spawns a fresh instance rather than delivering", %{
    registry: registry
  } do
    assert {:spawned, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    first = instance_pid(registry, "w1")

    :ets.delete(registry, "w1")

    assert {:spawned, "w1"} =
             spawn_or_deliver(@factory, "p2", :deliver)

    refute instance_pid(registry, "w1") == first
  end

  test "a factory no connected runner registers is reported, not spawned" do
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)

    assert {{:refused, %Flezha.Error{code: "relay.factory_unknown"}}, "w1"} =
             spawn_or_deliver("nobody", "p1", :deliver)
  end

  test "a factory its runners announced as a stream is named, not spawned" do
    stub(FleetManager, :factory_run, fn _ns, _factory -> {:ok, :flow} end)

    assert {{:refused, %Flezha.Error{code: "relay.factory_not_steps"}}, "w1"} =
             spawn_or_deliver("prices", "p1", :deliver)

    assert FakeStartRegistry.started() == []
  end

  test "a start forwarded to a node whose runner has gone answers factory_unknown" do
    # The directory still names a peer for the factory and the peer has lost its
    # runner. The pick lands on this node, so the real forward runs and the
    # refusal comes back exactly as a peer's would.
    stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)
    stub(Directory, :factory_nodes, fn _ns, _factory -> [:"stale-peer@127.0.0.1"] end)
    stub(Placement, :pick_runner, fn _nodes, _id -> node() end)

    assert {{:refused, %Flezha.Error{code: "relay.factory_unknown"}}, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert FakeStartRegistry.started() == []
  end

  test "an exhausted quota refuses the spawn with the reason attached" do
    stub(Quotas, :check_allocation, fn _ns, key, _n ->
      {:error, {:quota_exceeded, key, %{limit: 1}}}
    end)

    assert {{:refused, reason}, "w1"} =
             spawn_or_deliver(@factory, "p1", :deliver)

    assert %Flezha.Error{code: "quota.exceeded", details: %{"limit" => 1}} = reason
  end

  describe "a tasklet's request with an identity" do
    defp identified(id, payload, if_running, identity),
      do:
        RelayManager.spawn_or_deliver(
          @namespace,
          id,
          @factory,
          payload,
          if_running,
          @caller,
          identity
        )

    # A crash re-run asks again with the same identity: the generated id is
    # the identity's, so it resolves to the instance the first request started.
    test "a repeat without an id resolves to the instance the first started", %{
      registry: registry
    } do
      assert {:spawned, id} = identified("", "p1", :deliver, "origin-7")
      assert id =~ ~r/\Aworker-[a-z2-7]{16}\z/
      assert {:spawned, ^id} = identified("", "p1", :deliver, "origin-7")

      assert [%{id: ^id} = started] = FakeStartRegistry.started()
      assert Keyword.get(started.opts, :seed_input) == {"p1", %{"flz.msg_id" => "origin-7"}}
      assert Instance.ingested(instance_pid(registry, id)) == []

      assert {:spawned, other} = identified("", "p1", :deliver, "origin-8")
      refute other == id, "another origin is another request"
    end

    test "a repeat of a completed instance answers it and starts nothing", %{
      registry: registry
    } do
      {:spawned, id} = identified("", "p1", :deliver, "origin-9")
      [_] = FakeStartRegistry.started()
      :ets.delete(registry, id)
      stub(Flezha.State.Completion, :exists?, fn class -> class == id end)

      assert {:spawned, ^id} = identified("", "p1", :deliver, "origin-9")
      assert FakeStartRegistry.started() == [], "nothing started again"
    end

    test "a delivery to a named instance carries the identity as its message id", %{
      registry: registry
    } do
      assert {:spawned, "w1"} = identified("w1", "p1", :deliver, "origin-1")
      assert {:delivered, "w1"} = identified("w1", "p2", :deliver, "origin-2")

      assert Instance.ingested_with_metadata(instance_pid(registry, "w1")) == [
               {"p2", %{"flz.msg_id" => "origin-2"}}
             ]
    end

    test "a named repeat whose identity its log retains is answered from it" do
      stub(Flezha.State.InputLog, :msg_offset, fn
        "w1", "seeded" -> 0
        "w1", "delivered" -> 4
        _id, _identity -> nil
      end)

      assert {:spawned, "w1"} = identified("w1", "p", :error, "seeded")
      assert {:delivered, "w1"} = identified("w1", "p", :error, "delivered")
      assert FakeStartRegistry.started() == []
    end
  end

  describe "a request without an id" do
    test "spawns under a generated id and ignores the error policy", %{registry: registry} do
      assert {:spawned, first} =
               RelayManager.spawn_or_deliver(@namespace, "", @factory, "p1", :error, @caller)

      assert {:spawned, second} =
               RelayManager.spawn_or_deliver(@namespace, "", @factory, "p2", :error, @caller)

      assert first =~ ~r/\Aworker-[a-z2-7]{16}\z/
      assert second =~ ~r/\Aworker-[a-z2-7]{16}\z/
      refute first == second

      assert [%{id: ^first} = a, %{id: ^second} = b] = FakeStartRegistry.started()
      assert Keyword.get(a.opts, :seed_input) == {"p1", %{}}
      assert Keyword.get(b.opts, :seed_input) == {"p2", %{}}
      assert Instance.ingested(instance_pid(registry, first)) == []
    end

    test "is never merged with a concurrent request that carries none" do
      outcomes =
        1..8
        |> Task.async_stream(
          fn n ->
            RelayManager.spawn_or_deliver(
              @namespace,
              "",
              @factory,
              "p#{n}",
              :deliver,
              @caller
            )
          end,
          max_concurrency: 8,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, {:spawned, id}} -> id end)

      assert length(Enum.uniq(outcomes)) == 8
    end

    test "a factory nobody registers answers the empty id it carried" do
      stub(FleetManager, :runners_for_factory, fn _ns, _factory -> [] end)

      assert {{:refused, %Flezha.Error{code: "relay.factory_unknown"}}, ""} =
               RelayManager.spawn_or_deliver(
                 @namespace,
                 "",
                 "nobody",
                 "p1",
                 :deliver,
                 @caller
               )
    end
  end

  describe "start authority — the one id claim (D6)" do
    defp builder(id) do
      %Flezha.Relay.Builder{id: id, factory_id: @factory, init_values: %{}}
    end

    test "a builder naming no run takes the one its factory's runners announced" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("k1"))
      assert [%{run: :steps}] = FakeStartRegistry.started()
    end

    test "a run the caller asserts is judged against the factory's, never taken" do
      assert {:error, {:factory_run_mismatch, :flow, @namespace, _factory, :steps}} =
               RelayManager.start_relay(@namespace, %{builder("k2") | run: :flow})

      assert FakeStartRegistry.started() == []
    end

    test "a named start is never redrawn and never asks the generator" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("r1"))
      mint = fn _factory -> flunk("a named start drew an id") end

      assert :duplicate = RelayManager.start_relay(@namespace, builder("r1"), mint: mint)
    end

    test "a start that loses the claim concedes; the winner is left strictly alone", %{
      registry: registry
    } do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("r1"))
      winner = instance_pid(registry, "r1")

      # Stopping the id here would tear down the instance that WON the claim —
      # the start-authority violation the duplicate verdict exists to prevent.
      reject(&ProcessHub.stop_children/3)

      assert :duplicate = RelayManager.start_relay(@namespace, builder("r1"))
      assert instance_pid(registry, "r1") == winner
      assert Process.alive?(winner)
    end

    test "a claim refused by the registry's own check also concedes", %{registry: registry} do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("r1"))
      winner = instance_pid(registry, "r1")

      # The pre-check (`Validator.relay_unique/2`) sees the row first, so
      # this is the branch a non-racing duplicate start takes.
      reject(&ProcessHub.stop_children/3)
      assert :duplicate = RelayManager.start_relay(@namespace, builder("r1"))
      assert Process.alive?(winner)
    end

    test "a refusal with no live winner behind it stays an error" do
      # A registry row whose process is gone: conceding would silently accept a
      # start that produced nothing.
      stub(ProcessHub, :child_lookup, fn _hub, id -> {id, [{node(), self()}]} end)
      stub(HubManager, :get_pid, fn ns, id -> {:error, {:relay_not_found, ns, id}} end)

      assert {:error, reason} =
               RelayManager.start_relay(@namespace, builder("ghost"))

      assert reason == {:id_in_use, "ghost"}
    end

    test "a concurrent burst of plain starts yields exactly one instance" do
      outcomes =
        1..8
        |> Task.async_stream(
          fn _n -> RelayManager.start_relay(@namespace, builder("r_race")) end,
          max_concurrency: 8,
          timeout: 10_000
        )
        |> Enum.map(fn {:ok, outcome} -> outcome end)

      assert Enum.count(outcomes, &match?({:ok, _}, &1)) == 1
      assert Enum.count(outcomes, &(&1 == :duplicate)) == 7
    end
  end

  describe "a start without an id" do
    # A generator that answers `ids` in order, then fails the test if drawn again.
    defp minting(ids) do
      {:ok, agent} = Agent.start_link(fn -> ids end)

      fn _factory ->
        Agent.get_and_update(agent, fn
          [id | rest] -> {id, rest}
          [] -> flunk("drew more ids than the test offered")
        end)
      end
    end

    defp unnamed, do: builder(nil)

    test "starts under a generated id and answers it" do
      assert {:ok, %{relay_id: id}} = RelayManager.start_relay(@namespace, unnamed())

      assert id =~ ~r/\Aworker-[a-z2-7]{16}\z/
      assert [%{id: ^id}] = FakeStartRegistry.started()
    end

    test "a generated id a live relay holds is drawn again", %{registry: registry} do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("worker-live"))
      winner = instance_pid(registry, "worker-live")
      _ = FakeStartRegistry.started()
      reject(&ProcessHub.stop_children/3)

      assert {:ok, %{relay_id: "worker-free"}} =
               RelayManager.start_relay(@namespace, unnamed(),
                 mint: minting(["worker-live", "worker-free"])
               )

      assert instance_pid(registry, "worker-live") == winner
      assert Process.alive?(winner)
    end

    test "a generated id with a stale registry row is drawn again" do
      stub(ProcessHub, :child_lookup, fn _hub, id ->
        if id == "worker-ghost", do: {id, [{node(), self()}]}, else: nil
      end)

      assert {:ok, %{relay_id: "worker-free"}} =
               RelayManager.start_relay(@namespace, unnamed(),
                 mint: minting(["worker-ghost", "worker-free"])
               )
    end

    test "a generated id whose state folder is still on disk is drawn again" do
      left = Path.join(Paths.namespace_dir(@namespace), "worker-left")
      File.mkdir_p!(Path.join(left, "journal"))
      on_exit(fn -> File.rm_rf!(Paths.namespace_dir(@namespace)) end)

      assert {:ok, %{relay_id: "worker-free"}} =
               RelayManager.start_relay(@namespace, unnamed(),
                 mint: minting(["worker-left", "worker-free"])
               )

      assert File.dir?(Path.join(left, "journal"))
    end

    test "a named start over its own state folder is the ordinary restore" do
      File.mkdir_p!(Path.join(Paths.namespace_dir(@namespace), "restored"))
      on_exit(fn -> File.rm_rf!(Paths.namespace_dir(@namespace)) end)

      assert {:ok, %{relay_id: "restored"}} =
               RelayManager.start_relay(@namespace, builder("restored"))
    end

    test "a start whose every draw is taken is refused naming the exhaustion" do
      assert {:ok, _} = RelayManager.start_relay(@namespace, builder("worker-live"))

      assert {:error, reason} =
               RelayManager.start_relay(@namespace, unnamed(),
                 mint: minting(["worker-live", "worker-live", "worker-live"])
               )

      assert reason == {:no_free_id, "worker", 3}
    end

    test "a refusal that is not about the id is answered, not redrawn" do
      stub(Quotas, :check_allocation, fn _ns, key, _n ->
        {:error, {:quota_exceeded, key, %{limit: 1}}}
      end)

      assert {:error, {:quota_exceeded, :max_relays, _}} =
               RelayManager.start_relay(@namespace, unnamed(), mint: minting(["worker-a"]))
    end

    test "the published door answers the id and the outcome" do
      assert {:ok, %{id: "named", outcome: :started}} =
               @namespace
               |> RelayManager.start_relay(builder("named"))
               |> RelayManager.start_answer("named")

      assert {:ok, %{id: "named", outcome: :already_running}} =
               @namespace
               |> RelayManager.start_relay(builder("named"))
               |> RelayManager.start_answer("named")
    end
  end
end
