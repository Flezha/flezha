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
    def run(input, s, ctx), do: (Flezha.SDK.Ctx.emit(ctx, input); {:ok, s})
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "echo_op"
    def channels(c), do: c |> Channels.input("in") |> Channels.into(EchoProc) |> Channels.named("e") |> Channels.sink("out")
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
      c |> Channels.input("in") |> Channels.key_by(fn _ -> "k" end) |> Channels.into(OrderedProc) |> Channels.named("o") |> Channels.sink("out")
    end
  end

  defmodule IdentityKeyOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "idkey_op"
    def channels(c) do
      c |> Channels.input("in") |> Channels.key_by(fn v -> v end) |> Channels.into(OrderedProc) |> Channels.named("o") |> Channels.sink("out")
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
    def channels(c), do: c |> Channels.input("in") |> Channels.into(FlakyProc) |> Channels.named("f") |> Channels.sink("out")
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
    def channels(c), do: c |> Channels.input("in") |> Channels.into(PoisonProc) |> Channels.named("p") |> Channels.sink("out")
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
    on_exit(fn -> if Process.whereis(:dispatch_probe), do: Process.unregister(:dispatch_probe) end)

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
    assert_receive {:"$gen_cast", {:park, entity_pid, body}}, 1_000
    assert body.error =~ "always"
    assert body.payload == "poison"

    # releasing lets the Entity resume (a fresh input dispatches)
    Entity.release_park(entity_pid, body.park_id)
    Operative.input(op, "in", "poison2", %{})
    assert_receive {:"$gen_cast", {:park, ^entity_pid, _body2}}, 1_000
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
      def channels(c), do: c |> Channels.input("in") |> Channels.on_full(:drop_newest) |> Channels.into(SlowProc) |> Channels.named("s") |> Channels.sink("out")
    end

    %{op: op} = start_operative(DropOp)
    # Fire many quickly; capacity 1 + drop_newest → only a subset get through.
    for n <- 1..10, do: Operative.input(op, "in", "v#{n}", %{})

    outputs = drain_outputs([], 400)
    assert length(outputs) >= 1
    assert length(outputs) < 10
  end

  test "a mis-routed dynamic key parks with the contract error before any Entity state" do
    range = KeyGroup.decode(0, 2, 0, 127)
    %{op: op} = start_operative(IdentityKeyOp, range: range, runner: self())
    # "BTC-USD" hashes to key group 129 — outside this partition's 0..127 range.
    Operative.input(op, "in", "BTC-USD", %{})
    assert_receive {:"$gen_cast", {:park, _entity, body}}, 1_000
    assert body.error =~ "does not own"
  end

  defp drain_outputs(acc, timeout) do
    receive do
      {:flz_data, _id, "op-1", payload, _meta} -> drain_outputs([payload | acc], timeout)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end
