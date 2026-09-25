defmodule Flezha.SDK.DispatchTest do
  @moduledoc """
  Exercises the SDK dispatch runtime (Operator router + Cell mailbox/ladder)
  in isolation — no controller, no real transport. Sink output is captured on a
  native `LocalStream` whose controller side the test owns; `run/3` runs in the
  test's BEAM so it can also probe directly.
  """
  # async: false: the live controller and its fleet; registered singletons; node-global tables.
  use ExUnit.Case, async: false

  alias Flezha.SDK.{Channels, Counters, Cell, KeyGroup}
  alias Flezha.SDK.Ctx
  alias Flezha.SDK.Operator.Inbox
  alias Flezha.SDK.Wire
  alias FlezhaTest.Helper.{NodeTestHelper, SdkOperatorHarness}

  # --- fixtures ---

  defmodule EchoTask do
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "echo_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(EchoTask)
        |> Channels.named("e")
        |> Channels.sink()
  end

  defmodule MetaEchoTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "meta_echo"
    def init_state(_), do: nil

    def run(input, s, ctx) do
      :ok = Ctx.emit_with_meta(ctx, input, %{"tag" => "x"})
      {:ok, s}
    end
  end

  defmodule UnnamedMetaOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "unnamed_meta_op"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.into(MetaEchoTask) |> Channels.sink()
  end

  defmodule OrderedTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "ordered"
    def init_state(_), do: nil

    def run(input, s, _ctx) do
      send(:dispatch_probe |> Process.whereis(), {:ran, input})
      {:ok, s}
    end
  end

  defmodule OrderedOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "ordered_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.key_by(fn _ -> "k" end)
      |> Channels.into(OrderedTask)
      |> Channels.named("o")
      |> Channels.sink()
    end
  end

  defmodule IdentityKeyOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "idkey_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.key_by(fn v -> v end)
      |> Channels.into(OrderedTask)
      |> Channels.named("o")
      |> Channels.sink()
    end
  end

  defmodule FlakyTask do
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "flaky_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(FlakyTask)
        |> Channels.named("f")
        |> Channels.sink()
  end

  defmodule PoisonTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "poison"
    def init_state(_), do: nil
    def run(_input, _s, _ctx), do: {:error, {:non_retryable, "always"}}
  end

  defmodule RaiserTask do
    @behaviour Flezha.SDK.Tasklet
    def name, do: "raiser"
    def init_state(_), do: nil
    def run(_input, _s, _ctx), do: raise("boom")
  end

  defmodule RaiserOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "raiser_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.retry_max(3)
        |> Channels.retry_backoff_ms(1)
        |> Channels.into(RaiserTask)
        |> Channels.named("r")
        |> Channels.sink()
  end

  defmodule PoisonOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "poison_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(PoisonTask)
        |> Channels.named("p")
        |> Channels.sink()
  end

  # Blocks until released, so a dispatch can be observed mid-flight; the input
  # `kill` ends the Cell process untrappably, which no `after` can cover.
  defmodule GatedTask do
    @behaviour Flezha.SDK.Tasklet
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
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "gated_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(GatedTask)
        |> Channels.named("g")
        |> Channels.sink()
  end

  # Two entries merged into one tasklet, each source's mapping tagging its
  # value, keyed on the symbol after the tag: an unmapped value keys elsewhere.
  defmodule MergeOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "merge_op"

    def channels(c) do
      ticks = Channels.input(c, "ticks")
      quotes = Channels.input(c, "quotes")

      c
      |> Channels.merge()
      |> Channels.from(ticks, &"ticks:#{&1}")
      |> Channels.from(quotes, &"quotes:#{&1}")
      |> Channels.key_by(&(&1 |> String.split(":") |> Enum.at(1)))
      |> Channels.into(EchoTask)
      |> Channels.named("labels")
      |> Channels.sink()
    end
  end

  defmodule FanOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "fan_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.flat_map(&String.split(&1, ",", trim: true))
      |> Channels.key_by(&(&1 |> String.split(":") |> hd()))
      |> Channels.into(EchoTask)
      |> Channels.named("fanned")
      |> Channels.sink()
    end
  end

  # --- helpers ---

  defdelegate start_operator(op_module, opts \\ []), to: SdkOperatorHarness
  defdelegate cell!(op, tasklet, key), to: SdkOperatorHarness
  defdelegate rebind_secondary(op), to: SdkOperatorHarness
  defdelegate close_secondary(handle), to: SdkOperatorHarness

  defp payloads(n), do: n |> SdkOperatorHarness.frames() |> Enum.map(&elem(&1, 1))

  # --- tests ---

  test "full dispatch routes input → run → emit → sink output on a secondary" do
    %{op: op} = start_operator(EchoOp)
    Inbox.input(op, "in", "hello", %{})
    # The frame names the producing node's channel, never the plugin.
    assert_receive {:flz_data, _id, "op-1", "hello", %{"channel" => "e"}}, 1_000
  end

  test "the hold is bounded, and what it sheds is counted where every drop-oldest is" do
    %{op: op, counters: counters, sdk_secondary: sdk} = start_operator(EchoOp, hold_cap: 2)
    :ok = close_secondary(sdk)
    for n <- 1..5, do: Inbox.input(op, "in", "v#{n}", %{})

    # An input reaches the hold only once its Cell reports the dispatch back,
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

  test "an unnamed node's frame carries its tasklet name, and user metadata rides it" do
    %{op: op} = start_operator(UnnamedMetaOp)
    Inbox.input(op, "in", "hello", %{})
    assert_receive {:flz_data, _id, "op-1", "hello", meta}, 1_000
    assert meta == %{"channel" => "meta_echo", "tag" => "x"}
  end

  test "per-Cell inputs are processed in arrival order" do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)

    %{op: op} = start_operator(OrderedOp)
    for n <- 1..5, do: Inbox.input(op, "in", "v#{n}", %{})

    for n <- 1..5 do
      expected = "v#{n}"
      assert_receive {:ran, ^expected}, 1_000
    end
  end

  test "two merged entries reach one tasklet under their own mappings, keyed on the mapped value" do
    %{op: op} = start_operator(MergeOp)
    Inbox.input(op, "ticks", "AAPL:1", %{})
    Inbox.input(op, "quotes", "AAPL:2", %{})

    assert Enum.sort(payloads(2)) == ["quotes:AAPL:2", "ticks:AAPL:1"]
    assert is_pid(cell!(op, "echo", "AAPL"))
  end

  test "each merged source keeps its own order" do
    %{op: op} = start_operator(MergeOp)

    for n <- 1..3 do
      Inbox.input(op, "ticks", "MSFT:#{n}", %{})
      Inbox.input(op, "quotes", "MSFT:#{n}", %{})
    end

    released = payloads(6)

    assert Enum.filter(released, &String.starts_with?(&1, "ticks:")) ==
             ~w(ticks:MSFT:1 ticks:MSFT:2 ticks:MSFT:3)

    assert Enum.filter(released, &String.starts_with?(&1, "quotes:")) ==
             ~w(quotes:MSFT:1 quotes:MSFT:2 quotes:MSFT:3)
  end

  test "a fan of three runs the tasklet three times under three keys, and a fan of none is a drop" do
    %{op: op, counters: counters} = start_operator(FanOp)
    Inbox.input(op, "in", "", %{})
    Inbox.input(op, "in", "a:1,b:2,c:3", %{})

    assert Enum.sort(payloads(3)) == ["a:1", "b:2", "c:3"]
    assert NodeTestHelper.wait_until(fn -> Counters.dispatches(counters) == 3 end, 1_000)
    for key <- ~w(a b c), do: assert(is_pid(cell!(op, "echo", key)))
    refute_receive {:flz_data, _id, "op-1", _payload, _meta}, 200
    assert Counters.dispatches(counters) == 3
  end

  test "a transient failure recovers within the retry budget (no park)" do
    :persistent_term.put(:flaky_counter, :counters.new(1, []))
    on_exit(fn -> :persistent_term.erase(:flaky_counter) end)

    %{op: op, counters: counters} = start_operator(FlakyOp)
    Inbox.input(op, "in", "x", %{})

    # third attempt succeeds → one completed dispatch, output emitted
    assert_receive {:flz_data, _id, "op-1", "x", %{"channel" => "f"}}, 2_000
    assert Counters.dispatches(counters) == 1
  end

  test "a non-retryable failure parks immediately and the ack releases the Cell" do
    %{op: op} = start_operator(PoisonOp, runner: self())
    Inbox.input(op, "in", "poison", %{})

    # Operator forwards the park to its runner (this test process).
    assert_receive {:"$gen_cast", {:park, cell_pid, identity, body}}, 1_000
    assert body.error =~ "always"
    assert body.payload == "poison"

    # The park is keyed by the Cell the controller must file it under.
    assert {:ok, {"op-1", "poison", ""}} = Wire.decode_cell_id(identity)

    # releasing lets the Cell resume (a fresh input dispatches)
    Cell.release_park(cell_pid, body.park_id)
    Inbox.input(op, "in", "poison2", %{})
    assert_receive {:"$gen_cast", {:park, ^cell_pid, _identity2, _body2}}, 1_000
  end

  test "a raised invocation leaves no ctx of its own in the Cell's dictionary" do
    %{op: op} = start_operator(RaiserOp, runner: self())
    Inbox.input(op, "in", "boom", %{})

    # The park is the positive settle: every attempt of the ladder has run and
    # the Cell is idle, so what the dictionary holds now is what it keeps.
    assert_receive {:"$gen_cast", {:park, cell_pid, _identity, body}}, 2_000
    assert body.attempts == 3

    {:dictionary, dict} = Process.info(cell_pid, :dictionary)
    assert Enum.filter(dict, fn {k, _} -> is_tuple(k) and elem(k, 0) == Ctx end) == []
  end

  # `GatedTask` with a one-item mailbox: each run is held until the test
  # releases it, so a burst finds the mailbox full for as long as the test
  # wants rather than for as long as a sleep lasts.
  defmodule SlowTask do
    @behaviour Flezha.SDK.Tasklet
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
      @behaviour Flezha.SDK.UnboundedOperator
      def factory_id, do: "drop_op"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.on_full(:drop_newest)
          |> Channels.into(SlowTask)
          |> Channels.named("s")
          |> Channels.sink()
    end

    probe!()
    %{op: op, counters: counters} = start_operator(DropOp)
    # Fire many quickly; capacity 1 + drop_newest → only a subset get through.
    for n <- 1..10, do: Inbox.input(op, "in", "v#{n}", %{})

    # Every input either ran (released here as it reports in) or was shed:
    # the two accounts add up to the burst once it is through.
    runs = release_runs_until(fn runs -> runs + Counters.drops_newest(counters) == 10 end, 200)
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
    defmodule DefaultSlowTask do
      @behaviour Flezha.SDK.Tasklet
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
      @behaviour Flezha.SDK.UnboundedOperator
      def factory_id, do: "default_op"

      # No `on_full/2`: the kind-independent default is `:drop_nothing`, in
      # lockstep with the Rust and Java builders and the FFI host.
      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.into(DefaultSlowTask)
          |> Channels.named("s")
          |> Channels.sink()
    end

    probe!()
    %{op: op} = start_operator(DefaultOp)
    # The same burst `drop_newest` sheds: capacity 1 against 10 inputs, each
    # held at the gate. Under the default the router waits instead of dropping.
    for n <- 1..10, do: Inbox.input(op, "in", "v#{n}", %{})

    # Released as each reports in — the router is parked on the full mailbox
    # between runs, and an output is delivered only once it moves again.
    assert release_runs_until(fn runs -> runs == 10 end, 0) == 10
    assert length(collect_outputs(10)) == 10
  end

  test "a mis-routed dynamic key parks with the contract error before any Cell state" do
    range = KeyGroup.decode(0, 2, 0, 127)
    %{op: op} = start_operator(IdentityKeyOp, range: range, runner: self())
    # "BTC-USD" hashes to key group 129 — outside this partition's 0..127 range.
    Inbox.input(op, "in", "BTC-USD", %{})
    assert_receive {:"$gen_cast", {:park, _cell, _identity, body}}, 1_000
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

    # The self-report every SDK carries: one wedged Cell is invisible to
    # `dispatches` while the runner as a whole keeps completing work.
    test "a dispatch in flight is reported, and an idle runner reports zero" do
      %{op: op, counters: counters} = start_operator(GatedOp)
      assert Counters.handout_liveness(counters) == {0, 0}

      Inbox.input(op, "in", "slow", %{})
      assert_receive {:running, "slow", cell}, 1_000

      # The handout ages on the clock while the Cell holds it.
      assert NodeTestHelper.wait_until(
               fn ->
                 match?({1, oldest_ms} when oldest_ms >= 30, Counters.handout_liveness(counters))
               end,
               1_000,
               5
             )

      send(cell, :release)
      assert_receive {:flz_data, _id, "op-1", "slow", _}, 2_000

      assert NodeTestHelper.wait_until(
               fn -> Counters.handout_liveness(counters) == {0, 0} end,
               2_000,
               10
             )
    end

    # `after` cannot run for an untrappable kill, so the Operator releases the
    # dead Cell's slot — otherwise the runner would report a stall forever.
    test "a Cell killed mid-dispatch releases its slot, and its key is routable again" do
      %{op: op, counters: counters} = start_operator(GatedOp)

      Inbox.input(op, "in", "kill", %{})
      assert_receive {:running, "kill", cell}, 1_000
      assert {1, _} = Counters.handout_liveness(counters)

      ref = Process.monitor(cell)
      # `:noproc` when the kill landed before the monitor did — either way the
      # Cell is gone, which is what the slot release hangs off.
      assert_receive {:DOWN, ^ref, :process, ^cell, _reason}, 1_000

      assert NodeTestHelper.wait_until(
               fn -> Counters.handout_liveness(counters) == {0, 0} end,
               2_000,
               10
             ),
             "a killed Cell left its in-flight slot claimed"

      assert Process.alive?(op), "a crashed Cell must not take its Operator down"

      # The key is routable again: a fresh Cell is started on the next input.
      Inbox.input(op, "in", "after", %{})
      assert_receive {:running, "after", fresh}, 1_000
      send(fresh, :release)
      assert_receive {:flz_data, _id, "op-1", "after", _}, 2_000
    end
  end

  # The exit signal and the next input are two messages racing for the
  # Operator's mailbox. When the input wins, a `:block` enqueue is a
  # `GenServer.call` into a pid that is already dead — which took the Operator
  # down with it, leaving the key permanently unroutable.
  test "an input that beats a killed Cell's exit signal is still served" do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)

    %{op: op} = start_operator(GatedOp)
    Inbox.input(op, "in", "first", %{})
    assert_receive {:running, "first", cell}, 1_000
    send(cell, :release)
    assert_receive {:flz_data, _id, "op-1", "first", _}, 2_000

    # Force the losing order: the input is queued before the exit signal.
    :sys.suspend(op)
    Inbox.input(op, "in", "second", %{})
    Process.exit(cell, :kill)
    :sys.resume(op)

    assert_receive {:running, "second", fresh}, 2_000
    assert fresh != cell, "the input was handed to the dead Cell"
    send(fresh, :release)
    assert_receive {:flz_data, _id, "op-1", "second", _}, 2_000
    assert Process.alive?(op), "a dead Cell took its Operator down"
  end

  # This process is where a gated Tasklet reports in.
  defp probe!() do
    Process.register(self(), :dispatch_probe)

    on_exit(fn ->
      if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe)
    end)
  end

  # Release every gated run as it reports in, until `done?.(runs)` holds;
  # answers how many ran. `settle_ms` is how long `done?` may take to hold with
  # no further run: a drop count settles on its own, a run count never does.
  defp release_runs_until(done?, settle_ms, runs \\ 0) do
    if NodeTestHelper.wait_until(fn -> done?.(runs) end, settle_ms, 5) do
      runs
    else
      assert_receive {:running, _input, cell}, 1_000
      send(cell, :release)
      release_runs_until(done?, settle_ms, runs + 1)
    end
  end

  defp collect_outputs(n) do
    for _ <- 1..n do
      assert_receive {:flz_data, _id, "op-1", payload, _meta}, 1_000
      payload
    end
  end
end
