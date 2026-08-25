defmodule Flezha.SDK.DispatchTest do
  @moduledoc """
  Exercises the SDK dispatch runtime (Operative router + Entity mailbox/ladder)
  in isolation — no controller, no real transport. Sink output is captured on a
  native `Local.Stream` whose controller side the test owns; `run/3` runs in the
  test's BEAM so it can also probe directly.
  """
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Topology, Counters, Operative, Entity, Channels, KeyGroup}
  alias Flezha.Transport.Local

  # --- fixtures ---

  defmodule EchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, s, ctx),
      do:
        (
          Flezha.SDK.Ctx.emit(ctx, input)
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
        |> Channels.sink("out")
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
      |> Channels.sink("out")
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
      |> Channels.sink("out")
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
        Flezha.SDK.Ctx.emit(ctx, input)
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
        |> Channels.sink("out")
  end

  defmodule PoisonProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "poison"
    def init_state(_), do: nil
    def run(_input, _s, _ctx), do: {:error, {:non_retryable, "always"}}
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
        |> Channels.sink("out")
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

      Flezha.SDK.Ctx.emit(ctx, input)
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
        |> Channels.sink("out")
  end

  # --- helpers ---

  defp start_operative(op_module, opts \\ []) do
    {:ok, topo} = Topology.build(op_module)
    {sdk_sec, ctrl_sec} = Local.Stream.open()
    :ok = Flezha.Transport.Stream.controlling_process(ctrl_sec, self())
    :ok = Flezha.Transport.Stream.set_active(ctrl_sec)

    counters = Counters.new()

    {:ok, pid} =
      Operative.start_link(
        operative_id: Keyword.get(opts, :operative_id, "op-1"),
        topology: topo,
        range: Keyword.get(opts, :range, KeyGroup.decode(0, 0, 0, 0)),
        counters: counters,
        runner: Keyword.get(opts, :runner, self()),
        secondaries: [sdk_sec]
      )

    %{op: pid, counters: counters, ctrl_secondary: ctrl_sec}
  end

  # --- tests ---

  test "full dispatch routes input → run → emit → sink output on a secondary" do
    %{op: op} = start_operative(EchoOp)
    Operative.input(op, "in", "hello", %{})
    assert_receive {:flz_data, _id, "op-1", "hello", %{"channel" => "out"}}, 1_000
  end

  test "per-Entity inputs are processed in arrival order" do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)

    %{op: op} = start_operative(OrderedOp)
    for n <- 1..5, do: Operative.input(op, "in", "v#{n}", %{})

    for n <- 1..5 do
      expected = "v#{n}"
      assert_receive {:ran, ^expected}, 1_000
    end
  end

  test "a transient failure recovers within the retry budget (no park)" do
    :persistent_term.put(:flaky_counter, :counters.new(1, []))
    on_exit(fn -> :persistent_term.erase(:flaky_counter) end)

    %{op: op, counters: counters} = start_operative(FlakyOp)
    Operative.input(op, "in", "x", %{})

    # third attempt succeeds → one completed dispatch, output emitted
    assert_receive {:flz_data, _id, "op-1", "x", %{"channel" => "out"}}, 2_000
    assert Counters.dispatches(counters) == 1
  end

  test "a non-retryable failure parks immediately and the ack releases the Entity" do
    %{op: op} = start_operative(PoisonOp, runner: self())
    Operative.input(op, "in", "poison", %{})

    # Operative forwards the park to its runner (this test process).
    assert_receive {:"$gen_cast", {:park, entity_pid, identity, body}}, 1_000
    assert body.error =~ "always"
    assert body.payload == "poison"

    # The park is keyed by the Entity the controller must file it under.
    assert {:ok, {"op-1", "poison", ""}} = Flezha.State.Wire.decode_entity_id(identity)

    # releasing lets the Entity resume (a fresh input dispatches)
    Entity.release_park(entity_pid, body.park_id)
    Operative.input(op, "in", "poison2", %{})
    assert_receive {:"$gen_cast", {:park, ^entity_pid, _identity2, _body2}}, 1_000
  end

  test "drop_newest sheds at capacity; the runner stays responsive" do
    defmodule SlowProc do
      @behaviour Flezha.SDK.Procedure
      def name, do: "slow"
      def init_state(_), do: nil

      def run(input, s, ctx) do
        Process.sleep(50)
        Flezha.SDK.Ctx.emit(ctx, input)
        {:ok, s}
      end

      def mailbox_capacity, do: 1
    end

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
          |> Channels.sink("out")
    end

    %{op: op} = start_operative(DropOp)
    # Fire many quickly; capacity 1 + drop_newest → only a subset get through.
    for n <- 1..10, do: Operative.input(op, "in", "v#{n}", %{})

    outputs = drain_outputs([], 400)
    assert length(outputs) >= 1
    assert length(outputs) < 10
  end

  test "the undeclared default discards nothing; every input arrives" do
    defmodule DefaultSlowProc do
      @behaviour Flezha.SDK.Procedure
      def name, do: "default_slow"
      def init_state(_), do: nil

      def run(input, s, ctx) do
        Process.sleep(50)
        Flezha.SDK.Ctx.emit(ctx, input)
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
          |> Channels.sink("out")
    end

    %{op: op} = start_operative(DefaultOp)
    # The same burst `drop_newest` sheds: capacity 1 against 10 inputs, each
    # held 50 ms. Under the default the router waits instead of dropping.
    for n <- 1..10, do: Operative.input(op, "in", "v#{n}", %{})

    assert length(drain_outputs([], 1_500)) == 10
  end

  test "a mis-routed dynamic key parks with the contract error before any Entity state" do
    range = KeyGroup.decode(0, 2, 0, 127)
    %{op: op} = start_operative(IdentityKeyOp, range: range, runner: self())
    # "BTC-USD" hashes to key group 129 — outside this partition's 0..127 range.
    Operative.input(op, "in", "BTC-USD", %{})
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

      Operative.input(op, "in", "slow", %{})
      assert_receive {:running, "slow", entity}, 1_000

      Process.sleep(40)
      assert {1, oldest_ms} = Counters.handout_liveness(counters)
      assert oldest_ms >= 30

      send(entity, :release)
      assert_receive {:flz_data, _id, "op-1", "slow", _}, 2_000
      assert wait_until(fn -> Counters.handout_liveness(counters) == {0, 0} end)
    end

    # `after` cannot run for an untrappable kill, so the Operative releases the
    # dead Entity's slot — otherwise the runner would report a stall forever.
    test "an Entity killed mid-dispatch releases its slot, and its key is routable again" do
      %{op: op, counters: counters} = start_operative(GatedOp)

      Operative.input(op, "in", "kill", %{})
      assert_receive {:running, "kill", entity}, 1_000
      assert {1, _} = Counters.handout_liveness(counters)

      ref = Process.monitor(entity)
      # `:noproc` when the kill landed before the monitor did — either way the
      # Entity is gone, which is what the slot release hangs off.
      assert_receive {:DOWN, ^ref, :process, ^entity, _reason}, 1_000

      assert wait_until(fn -> Counters.handout_liveness(counters) == {0, 0} end),
             "a killed Entity left its in-flight slot claimed"

      assert Process.alive?(op), "a crashed Entity must not take its Operative down"

      # The key is routable again: a fresh Entity is started on the next input.
      Operative.input(op, "in", "after", %{})
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
    Operative.input(op, "in", "first", %{})
    assert_receive {:running, "first", entity}, 1_000
    send(entity, :release)
    assert_receive {:flz_data, _id, "op-1", "first", _}, 2_000

    # Force the losing order: the input is queued before the exit signal.
    :sys.suspend(op)
    Operative.input(op, "in", "second", %{})
    Process.exit(entity, :kill)
    :sys.resume(op)

    assert_receive {:running, "second", fresh}, 2_000
    assert fresh != entity, "the input was handed to the dead Entity"
    send(fresh, :release)
    assert_receive {:flz_data, _id, "op-1", "second", _}, 2_000
    assert Process.alive?(op), "a dead Entity took its Operative down"
  end

  defp wait_until(done, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    Stream.repeatedly(fn ->
      done.() or (Process.sleep(10) && false)
    end)
    |> Enum.find(fn ok -> ok or System.monotonic_time(:millisecond) > deadline end)
  end

  defp drain_outputs(acc, timeout) do
    receive do
      {:flz_data, _id, "op-1", payload, _meta} -> drain_outputs([payload | acc], timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end
