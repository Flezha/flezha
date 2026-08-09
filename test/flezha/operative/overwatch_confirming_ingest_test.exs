defmodule FlezhaTest.Operative.OverwatchConfirmingIngestTest do
  @moduledoc """
  `operative-to-operative-messaging`: the confirming ingest entry point and its
  verdicts. Every case drives `Overwatch.handle_call/3` directly on a struct —
  the state machine is the unit here, as in `overwatch_suspension_test`.

  The companion assertion is that the existing `{:io_in, _, _}` cast is
  unchanged, including its silent drop for a parked operative: a sender that
  needs the payload held uses the confirming path instead.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.{Builder, Overwatch}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{ProcInbox, TestHelpers}

  setup :set_mimic_global

  setup do
    root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(root) end)
    :ok
  end

  defp instance_id(), do: "confirm_#{System.unique_integer([:positive])}"

  # Stand-in StreamChannel. The test process cannot be the channel here: it is
  # the one running the Overwatch callback, so a call to itself would deadlock.
  defp fake_channel(reply) do
    test_pid = self()

    spawn_link(fn ->
      receive do
        {:"$gen_call", {from, ref}, {:io_in, id, payload}} ->
          send(test_pid, {:channel_got, id, payload})
          send(from, {ref, reply})
      end
    end)
  end

  defp state(inst, overrides \\ []) do
    base = %Overwatch{
      id: inst,
      namespace: "default",
      runner_id: "ex_1",
      factory_id: "chat",
      builder: %Builder{id: inst, factory_id: "chat", kind: :unbounded, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__())
    }

    struct!(base, overrides)
  end

  defp with_filter(state, pattern) do
    plugin =
      Flezha.Plugin.new(
        id: "filter",
        provider: Flezha.Plugin.Provider.Filter,
        opts: %{regex_pattern: pattern, inverted: true},
        namespace: "default",
        operative_id: state.id,
        subscriptions: [{:on_input, []}]
      )

    {:ok, table} = Table.subscribe(state.extensions, "filter", plugin)
    %{state | extensions: table}
  end

  test "a healthy bound operative confirms :ok and the payload reaches the channel" do
    inst = instance_id()
    state = state(inst, channel: fake_channel(:ok))

    assert {:reply, :ok, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)

    assert_receive {:channel_got, ^inst, {"hello", _md}}
  end

  test "a suspended target confirms {:ok, :suspended} after a durable append" do
    inst = instance_id()
    test_pid = self()

    stub(ProcInbox, :append, fn ^inst, msg, _md ->
      send(test_pid, {:appended, msg})
      {:ok, 7}
    end)

    # No runner to rehydrate onto: the instance stays suspended and the payload
    # waits in the inbox, which is exactly the guarantee the verdict reports.
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    state = state(inst, durable_bounded: true, suspended: true, channel: nil)

    assert {:reply, {:ok, :suspended}, _state} =
             Overwatch.handle_call({:ingest, "turn", %{}}, self(), state)

    assert_receive {:appended, "turn"}
  end

  test "a parked target reports not-ready instead of dropping" do
    state = state(instance_id(), init_error: :sdk_start_failed)

    assert {:reply, {:error, :not_ready}, _state} =
             Overwatch.handle_call({:ingest, "hello", %{}}, self(), state)
  end

  test "a payload filtered by an :on_input plugin reports absorbed" do
    state = state(instance_id()) |> with_filter("drop-me")

    assert {:reply, {:ok, :absorbed}, _state} =
             Overwatch.handle_call({:ingest, "drop-me now", %{}}, self(), state)
  end

  test "a duplicate absorbed by the durable inbox reports absorbed" do
    inst = instance_id()
    stub(ProcInbox, :append, fn ^inst, _msg, _md -> {:duplicate, 3} end)

    state = state(inst, durable_bounded: true, channel: fake_channel(:ok))

    assert {:reply, {:ok, :absorbed}, _state} =
             Overwatch.handle_call({:ingest, "again", %{}}, self(), state)

    refute_receive {:channel_got, _, _}, 50
  end

  test "a durable-log refusal reports refused so the sender retries" do
    inst = instance_id()
    stub(ProcInbox, :append, fn ^inst, _msg, _md -> {:error, :enospc} end)

    state = state(inst, durable_bounded: true, channel: fake_channel(:ok))

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:reply, {:error, :refused}, _state} =
                 Overwatch.handle_call({:ingest, "doomed", %{}}, self(), state)
      end)

    # append-before-forward: the refusal is loud, and names why nothing moved.
    assert log =~ "durable-inbox append failed for #{inst}"
    assert log =~ ":enospc"
    assert log =~ "input NOT forwarded"

    refute_receive {:channel_got, _, _}, 50
  end

  test "an unbound stream is not-ready for a non-durable target, :ok for a durable one" do
    inst = instance_id()

    assert {:reply, {:error, :not_ready}, _} =
             Overwatch.handle_call({:ingest, "x", %{}}, self(), state(inst, channel: nil))

    stub(ProcInbox, :append, fn ^inst, _msg, _md -> {:ok, 1} end)

    durable = state(inst, durable_bounded: true, channel: nil)

    assert {:reply, :ok, _} = Overwatch.handle_call({:ingest, "x", %{}}, self(), durable)
  end

  test "an unresolvable target is not-found" do
    stub(Flezha.Service.HubManager, :get_pid, fn _ns, _id -> {:error, "no pid found"} end)

    assert {:error, :not_found} =
             Flezha.Service.OperativeManager.send_io_confirming(
               "ghost",
               "default",
               "hello",
               %{}
             )
  end

  test "the existing cast keeps its silent drop for a parked operative" do
    state = state(instance_id(), init_error: :sdk_start_failed, channel: fake_channel(:ok))

    assert {:noreply, ^state} = Overwatch.handle_cast({:io_in, "hello", %{}}, state)
    refute_receive {:channel_got, _, _}, 50
  end
end
