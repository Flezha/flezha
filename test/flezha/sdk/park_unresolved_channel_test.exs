defmodule Flezha.SDK.ParkUnresolvedChannelTest do
  @moduledoc """
  A frame naming a channel the operative does not declare is parked with the
  name it asked for, never delivered to whichever entry channel happens to be
  first. A frame naming no channel keeps taking the first declared entry, which
  is the source-plugin default and not a misconfiguration.

  The frames are cast straight through `Flezha.SDK.Operative.Inbox.input/4`,
  which is how one arrives off the wire; `Flezha.SDK.Local.feed/3` refuses an
  undeclared channel before it gets that far, so going through it would test
  the guard rather than the dispatch.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Ctx, Entity}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.State.Wire
  import FlezhaTest.Helper.SdkOperativeHarness

  defmodule ProcA do
    @behaviour Flezha.SDK.Procedure
    def name, do: "proc_a"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule ProcB do
    @behaviour Flezha.SDK.Procedure
    def name, do: "proc_b"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  # Two declared entry channels: "prices" (the first, and where an unresolved
  # name used to land) and "volumes".
  defmodule TwoEntryOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "q2_two_entry_op"

    def channels(c) do
      c
      |> Channels.input("prices")
      |> Channels.into(ProcA)
      |> Channels.named("a")
      |> Channels.sink()

      c
      |> Channels.input("volumes")
      |> Channels.into(ProcB)
      |> Channels.named("b")
      |> Channels.sink()
    end
  end

  # The same two entry channels, declared in the order an author wrote them
  # and NOT in the order a map's keys come back: "volumes" sorts after
  # "prices", so the first declared and the alphabetically first differ.
  defmodule ReverseOrderOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "q2_reverse_order_op"

    def channels(c) do
      c
      |> Channels.input("volumes")
      |> Channels.into(ProcB)
      |> Channels.named("b")
      |> Channels.sink()

      c
      |> Channels.input("prices")
      |> Channels.into(ProcA)
      |> Channels.named("a")
      |> Channels.sink()
    end
  end

  # A class that declares no entry channel at all: nothing to resolve, and
  # nothing to default to.
  defmodule NoEntryOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "q2_no_entry_op"
    def channels(c), do: c
  end

  # Every input this Entity was given has been answered: a read replies only
  # behind the casts already in its mailbox, so an output a reroute would have
  # produced is in this process's mailbox by the time this returns.
  defp settled(op, procedure),
    do: assert({:ok, _state, _seq} = Entity.read(entity!(op, procedure), 1_000))

  defp park(op, channel, message) do
    Inbox.input(op, channel, message, %{})

    assert_receive {:"$gen_cast", {:park, _entity_pid, identity, body}},
                   1_000,
                   "expected an unresolved channel name to be parked"

    assert {:ok, {_class, procedure_id, ""}} = Wire.decode_entity_id(identity)

    # The controller turns the procedure slot into a directory under the
    # dead-letter root, so a name off the wire must never reach it.
    assert procedure_id == "_unresolved",
           "the identity slot is this side's own, not the name the frame asked for"

    body
  end

  test "a frame naming an undeclared channel is parked under that name, not rerouted" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())

    body = park(op, "not_prices", "misrouted-bytes")

    assert body.channel == "not_prices", "the row carries the name the frame asked for"

    assert body.error ==
             "no entry channel `not_prices`; the operative declares: prices, volumes"

    assert body.payload == "misrouted-bytes", "the payload bytes are kept"
    assert body.attempts == 1, "the park is immediate"

    settled(op, "proc_a")
    refute_received {:flz_data, _id, _payload_id, _frame, _meta}
  end

  # `named/2` gives ProcA's output channel the name "a". It is a declared name
  # of this operative, and it is not an entry channel, so a frame asking for it
  # parks exactly as an undeclared name does — the same answer `flz_core` gives
  # through its kind-aware lookup.
  test "a frame naming a declared channel of another kind is parked too" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())

    body = park(op, "a", "wrong-kind-bytes")

    assert body.channel == "a"
    assert body.error == "no entry channel `a`; the operative declares: prices, volumes"

    settled(op, "proc_a")
    refute_received {:flz_data, _id, _payload_id, _frame, _meta}
  end

  test "a frame naming no channel still reaches the first declared entry channel" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())

    Inbox.input(op, nil, "defaulted", %{})

    # `named/2` is what names the channel an output rides; `sink/2` names the
    # plugin sink on it.
    assert_receive {:flz_data, _id, _payload_id, "defaulted", %{"channel" => "a"}}, 2_000
    refute_received {:"$gen_cast", {:park, _, _, _}}
  end

  test "a frame naming a declared channel reaches that one, not the first" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())

    Inbox.input(op, "volumes", "named", %{})

    assert_receive {:flz_data, _id, _payload_id, "named", %{"channel" => "b"}}, 2_000
    refute_received {:"$gen_cast", {:park, _, _, _}}
  end

  test "an operative declaring no entry channel parks with its own reason" do
    %{op: op} = start_operative(NoEntryOp, runner: self())

    assert park(op, nil, "nowhere-to-go").error == "the operative declares no entry channel"
  end

  # A metadata value is capped at 255 bytes on the wire, and that is exactly
  # what `Flezha.State.Wire.encode_entity_id/3` will take for the slot the
  # requested name rides in. The longest name a frame can carry must park
  # rather than raise there.
  # `flz_core::first_of_kind` walks the channels in declaration order, so the
  # Elixir SDK must too. `Map.keys/1` on `entries` comes back in TERM order,
  # which is why the default reads off `entry_order` instead.
  test "a frame naming no channel reaches the first DECLARED entry channel, not the first sorted" do
    %{op: op} = start_operative(ReverseOrderOp, runner: self())

    Inbox.input(op, nil, "defaulted", %{})

    assert_receive {:flz_data, _id, _payload_id, "defaulted", %{"channel" => "b"}}, 2_000
    refute_received {:"$gen_cast", {:park, _, _, _}}
  end

  test "a requested name that would climb out of a directory never reaches the identity" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())

    # `park/3` asserts the identity slot; this pins the row's attribution.
    assert park(op, "../../../../etc", "climbing-bytes").channel == "../../../../etc"
  end

  test "a requested name at the metadata cap parks rather than raising on the identity" do
    %{op: op} = start_operative(TwoEntryOp, runner: self())
    longest = String.duplicate("n", 255)

    assert park(op, longest, "long-name-bytes").channel == longest
  end
end
