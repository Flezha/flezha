defmodule Flezha.SDK.DispatchTest do
  @moduledoc """
  Exercises the SDK dispatch runtime (Operative router + Entity mailbox/ladder)
  in isolation — no controller, no real transport. Sink output is captured on a
  native `Local.Stream` whose controller side the test owns; `run/3` runs in the
  test's BEAM so it can also probe directly.
  """
  # async: false: the live controller and its fleet; registered singletons; node-global tables.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Counters, Entity, KeyGroup}
  alias Flezha.SDK.Ctx
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.State.Wire
  alias FlezhaTest.Helper.{NodeTestHelper, SdkOperativeHarness}

  # --- fixtures ---

  defmodule EchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx),
      do:
        (
          Ctx.emit(ctx, input)
          {:ok, s}
        )
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "echo_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(EchoProc)
        |> Channels.named("e")
        |> Channels.sink()
  end

  defmodule MetaEchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "meta_echo"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      :ok = Ctx.emit_with_meta(ctx, input, %{"tag" => "x"})
      {:ok, s}
    end
  end

  defmodule UnnamedMetaOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "unnamed_meta_op"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(MetaEchoProc) |> Channels.sink()
  end

  defmodule OrderedProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "ordered"
    def init_state(_), do: nil

    def run(input, s, _ctx) do
      send(:dispatch_probe |> Process.whereis(), {:ran, input})
      {:ok, s}
    end
  end

  defmodule OrderedOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "ordered_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.key_by(fn _ -> "k" end)
      |> Channels.into(OrderedProc)
      |> Channels.named("o")
      |> Channels.sink()
    end
  end

  defmodule IdentityKeyOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "idkey_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.key_by(fn v -> v end)
      |> Channels.into(OrderedProc)
      |> Channels.named("o")
      |> Channels.sink()
    end
  end

  defmodule FlakyProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "flaky"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      n = :counters.get(probe_counter(), 1)
      :counters.add(probe_counter(), 1, 1)

      if n < 2 do
        raise("boom #{n}")
      else
        Ctx.emit(ctx, input)
        {:ok, s}
      end
    end

    defp probe_counter, do: :persistent_term.get(:flaky_counter)
  end

  defmodule FlakyOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "flaky_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(FlakyProc)
        |> Channels.named("f")
        |> Channels.sink()
  end

  defmodule PoisonProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "poison"
    def init_state(_), do: nil
    def run(_input, _s, _ctx), do: {:error, {:non_retryable, "always"}}
  end

  defmodule RaiserProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "raiser"
    def init_state(_), do: nil
    def run(_input, _s, _ctx), do: raise("boom")
  end

  defmodule RaiserOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "raiser_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.retry_max(3)
        |> Channels.retry_backoff_ms(1)
        |> Channels.into(RaiserProc)
        |> Channels.named("r")
        |> Channels.sink()
  end

  defmodule PoisonOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "poison_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(PoisonProc)
        |> Channels.named("p")
        |> Channels.sink()
  end

  # Blocks until released, so a dispatch can be observed mid-flight; the input
  # `kill` ends the Entity process untrappably, which no `after` can cover.
  defmodule GatedProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "gated"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      send(:dispatch_probe, {:running, input, self()})
      if input == "kill", do: Process.exit(self(), :kill)

      receive do
        :release -> :ok
      after
        5_000 -> :ok
      end

      Ctx.emit(ctx, input)
      {:ok, s}
    end
  end

  defmodule GatedOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "gated_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(GatedProc)
        |> Channels.named("g")
        |> Channels.sink()
  end

  # --- helpers ---

  defdelegate start_operative(op_module, opts \\ []), to: SdkOperativeHarness
  defdelegate rebind_secondary(op), to: SdkOperativeHarness
  defdelegate close_secondary(handle), to: SdkOperativeHarness

  # --- tests ---

  test "full dispatch routes input → run → emit → sink output on a secondary" do
    %{op: op} = start_operative(EchoOp)
    Inbox.input(op, "in", "hello", %{})
    # The frame names the producing node's channel, never the plugin.
    assert_receive {:flz_data, _id, "op-1", "hello", %{"channel" => "e"}}, 1_000
  end

  test "the hold is bounded, and what it sheds is counted where every drop-oldest is" do
    %{op: op, counters: counters, sdk_secondary: sdk} = start_operative(EchoOp, hold_cap: 2)
    :ok = close_secondary(sdk)
    for n <- 1..5, do: Inbox.input(op, "in", "v#{n}", %{})

    # An input reaches the hold only once its Entity reports the dispatch back,
    # which is a round trip the five casts do not wait for. Three sheds is the
    # signal that all five are in: a rebind before that replays a short ring and
    # then delivers the rest live, which is the opposite of what this asserts.
    assert NodeTestHelper.wait_until(fn -> Counters.drops_oldest(counters) == 3 end, 2_000)

    rebind_secondary(op)

    # Drop-oldest: the last two survive, and the three shed are counted rather
    # than lost in silence.
    assert_receive {:flz_data, _id, "op-1", "v4", _}, 2_000
    assert_receive {:flz_data, _id, "op-1", "v5", _}, 2_000
    refute_receive {:flz_data, _id, "op-1", "v1", _}, 200
    assert Counters.drops_oldest(counters) == 3
  end

  test "an unnamed node's frame carries its procedure name, and user metadata rides it" do
    %{op: op} = start_operative(UnnamedMetaOp)
    Inbox.input(op, "in", "hello", %{})
    assert_receive {:flz_data, _id, "op-1", "hello", meta}, 1_000
    assert meta == %{"channel" => "meta_echo", "tag" => "x"}
  end

  test "per-Entity inputs are processed in arrival order" do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)

    %{op: op} = start_operative(OrderedOp)
    for n <- 1..5, do: Inbox.input(op, "in", "v#{n}", %{})

    for n <- 1..5 do
      expected = "v#{n}"
      assert_receive {:ran, ^expected}, 1_000
    end
  end

  test "a transient failure recovers within the retry budget (no park)" do
    :persistent_term.put(:flaky_counter, :counters.new(1, []))
    on_exit(fn -> :persistent_term.erase(:flaky_counter) end)

    %{op: op, counters: counters} = start_operative(FlakyOp)
    Inbox.input(op, "in", "x", %{})

    # third attempt succeeds → one completed dispatch, output emitted
    assert_receive {:flz_data, _id, "op-1", "x", %{"channel" => "f"}}, 2_000
    assert Counters.dispatches(counters) == 1
  end

  test "a non-retryable failure parks immediately and the ack releases the Entity" do
    %{op: op} = start_operative(PoisonOp, runner: self())
    Inbox.input(op, "in", "poison", %{})

    # Operative forwards the park to its runner (this test process).
    assert_receive {:"$gen_cast", {:park, entity_pid, identity, body}}, 1_000
    assert body.error =~ "always"
    assert body.payload == "poison"

    # The park is keyed by the Entity the controller must file it under.
    assert {:ok, {"op-1", "poison", ""}} = Wire.decode_entity_id(identity)

    # releasing lets the Entity resume (a fresh input dispatches)
    Entity.release_park(entity_pid, body.park_id)
    Inbox.input(op, "in", "poison2", %{})
    assert_receive {:"$gen_cast", {:park, ^entity_pid, _identity2, _body2}}, 1_000
  end

  test "a raised invocation leaves no ctx of its own in the Entity's dictionary" do
    %{op: op} = start_operative(RaiserOp, runner: self())
    Inbox.input(op, "in", "boom", %{})

    # The park is the positive settle: every attempt of the ladder has run and
    # the Entity is idle, so what the dictionary holds now is what it keeps.
    assert_receive {:"$gen_cast", {:park, entity_pid, _identity, body}}, 2_000
    assert body.attempts == 3

    {:dictionary, dict} = Process.info(entity_pid, :dictionary)
    assert Enum.filter(dict, fn {k, _} -> is_tuple(k) and elem(k, 0) == Ctx end) == []
  end

  # `GatedProc` with a one-item mailbox: each run is held until the test
  # releases it, so a burst finds the mailbox full for as long as the test
  # wants rather than for as long as a sleep lasts.
  defmodule SlowProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "slow"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      send(:dispatch_probe, {:running, input, self()})
      receive(do: (:release -> :ok))
      Ctx.emit(ctx, input)
      {:ok, s}
    end

    def mailbox_capacity, do: 1
  end

  test "drop_newest sheds at capacity; the runner stays responsive" do
    defmodule DropOp do
      @behaviour Flezha.SDK.StreamOperative
      def factory_id, do: "drop_op"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.on_full(:drop_newest)
          |> Channels.into(SlowProc)
          |> Channels.named("s")
          |> Channels.sink()
    end

    probe!()
    %{op: op, counters: counters} = start_operative(DropOp)
    # Fire many quickly; capacity 1 + drop_newest → only a subset get through.
    for n <- 1..10, do: Inbox.input(op, "in", "v#{n}", %{})

    # Every input either ran (released here as it reports in) or was shed:
    # the two accounts add up to the burst once it is through.
    runs = release_runs_until(fn runs -> runs + Counters.drops_newest(counters) == 10 end)
    outputs = collect_outputs(runs)
    assert outputs != []
    assert length(outputs) < 10

    # A shed input has to be countable from OUTSIDE the runner: the counter is
    # what the data heartbeat carries, the telemetry event is the in-node
    # handle, and the two must agree about the same drop.
    shed = Counters.drops_newest(counters)
    assert shed >= 1
    assert shed == 10 - length(outputs)
    assert Counters.drops_oldest(counters) == 0
  end

  test "the undeclared default discards nothing; every input arrives" do
    defmodule DefaultSlowProc do
      @behaviour Flezha.SDK.Procedure
      def name, do: "default_slow"
      def init_state(_), do: nil

      def run(input, s, ctx) do
        send(:dispatch_probe, {:running, input, self()})
        receive(do: (:release -> :ok))
        Ctx.emit(ctx, input)
        {:ok, s}
      end

      def mailbox_capacity, do: 1
    end

    defmodule DefaultOp do
      @behaviour Flezha.SDK.StreamOperative
      def factory_id, do: "default_op"

      # No `on_full/2`: the kind-independent default is `:drop_nothing`, in
      # lockstep with the Rust and Java builders and the FFI host.
      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.into(DefaultSlowProc)
          |> Channels.named("s")
          |> Channels.sink()
    end

    probe!()
    %{op: op} = start_operative(DefaultOp)
    # The same burst `drop_newest` sheds: capacity 1 against 10 inputs, each
    # held at the gate. Under the default the router waits instead of dropping.
    for n <- 1..10, do: Inbox.input(op, "in", "v#{n}", %{})

    # Released as each reports in — the router is parked on the full mailbox
    # between runs, and an output is delivered only once it moves again.
    assert release_runs_until(fn runs -> runs == 10 end) == 10
    assert length(collect_outputs(10)) == 10
  end

  test "a mis-routed dynamic key parks with the contract error before any Entity state" do
    range = KeyGroup.decode(0, 2, 0, 127)
    %{op: op} = start_operative(IdentityKeyOp, range: range, runner: self())
    # "BTC-USD" hashes to key group 129 — outside this partition's 0..127 range.
    Inbox.input(op, "in", "BTC-USD", %{})
    assert_receive {:"$gen_cast", {:park, _entity, _identity, body}}, 1_000
    assert body.error =~ "does not own"
  end

  describe "handout liveness" do
    setup do
      Process.register(self(), :dispatch_probe)

      on_exit(fn ->
        if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
      end)

      :ok
    end

    # The self-report every SDK carries: one wedged Entity is invisible to
    # `dispatches` while the runner as a whole keeps completing work.
    test "a dispatch in flight is reported, and an idle runner reports zero" do
      %{op: op, counters: counters} = start_operative(GatedOp)
      assert Counters.handout_liveness(counters) == {0, 0}

      Inbox.input(op, "in", "slow", %{})
      assert_receive {:running, "slow", entity}, 1_000

      # The handout ages on the clock while the Entity holds it.
      assert NodeTestHelper.wait_until(
               fn ->
                 match?({1, oldest_ms} when oldest_ms >= 30, Counters.handout_liveness(counters))
               end,
               1_000,
               5
             )

      send(entity, :release)
      assert_receive {:flz_data, _id, "op-1", "slow", _}, 2_000

      assert NodeTestHelper.wait_until(
               fn -> Counters.handout_liveness(counters) == {0, 0} end,
               2_000,
               10
             )
    end

    # `after` cannot run for an untrappable kill, so the Operative releases the
    # dead Entity's slot — otherwise the runner would report a stall forever.
    test "an Entity killed mid-dispatch releases its slot, and its key is routable again" do
      %{op: op, counters: counters} = start_operative(GatedOp)

      Inbox.input(op, "in", "kill", %{})
      assert_receive {:running, "kill", entity}, 1_000
      assert {1, _} = Counters.handout_liveness(counters)

      ref = Process.monitor(entity)
      # `:noproc` when the kill landed before the monitor did — either way the
      # Entity is gone, which is what the slot release hangs off.
      assert_receive {:DOWN, ^ref, :process, ^entity, _reason}, 1_000

      assert NodeTestHelper.wait_until(
               fn -> Counters.handout_liveness(counters) == {0, 0} end,
               2_000,
               10
             ),
             "a killed Entity left its in-flight slot claimed"

      assert Process.alive?(op), "a crashed Entity must not take its Operative down"

      # The key is routable again: a fresh Entity is started on the next input.
      Inbox.input(op, "in", "after", %{})
      assert_receive {:running, "after", fresh}, 1_000
      send(fresh, :release)
      assert_receive {:flz_data, _id, "op-1", "after", _}, 2_000
    end
  end

  # The exit signal and the next input are two messages racing for the
  # Operative's mailbox. When the input wins, a `:block` enqueue is a
  # `GenServer.call` into a pid that is already dead — which took the Operative
  # down with it, leaving the key permanently unroutable.
  test "an input that beats a killed Entity's exit signal is still served" do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)

    %{op: op} = start_operative(GatedOp)
    Inbox.input(op, "in", "first", %{})
    assert_receive {:running, "first", entity}, 1_000
    send(entity, :release)
    assert_receive {:flz_data, _id, "op-1", "first", _}, 2_000

    # Force the losing order: the input is queued before the exit signal.
    :sys.suspend(op)
    Inbox.input(op, "in", "second", %{})
    Process.exit(entity, :kill)
    :sys.resume(op)

    assert_receive {:running, "second", fresh}, 2_000
    assert fresh != entity, "the input was handed to the dead Entity"
    send(fresh, :release)
    assert_receive {:flz_data, _id, "op-1", "second", _}, 2_000
    assert Process.alive?(op), "a dead Entity took its Operative down"
  end

  # This process is where a gated Procedure reports in.
  defp probe!() do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)
  end

  # Release every gated run as it reports in, until `done?.(runs)` holds;
  # answers how many ran.
  defp release_runs_until(done?, runs \\ 0) do
    if NodeTestHelper.wait_until(fn -> done?.(runs) end, 200, 5) do
      runs
    else
      assert_receive {:running, _input, entity}, 1_000
      send(entity, :release)
      release_runs_until(done?, runs + 1)
    end
  end

  defp collect_outputs(n) do
    for _ <- 1..n do
      assert_receive {:flz_data, _id, "op-1", payload, _meta}, 1_000
      payload
    end
  end
end
