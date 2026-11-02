defmodule Flezha.Service.RelayActivityTest do
  @moduledoc """
  `Relay.activity`: the relay's Overwatch asks the runner hosting it, off its
  own process, and answers the rows with the read time, the instance and the
  runner; a relay that is not resident is refused without asking, and a
  runner that does not answer is named. `flz relay activity` prints it.
  """
  # async: false: Mimic global mode, Application env and the class registry.
  use FlezhaTest.Case, fixtures: [:env, :tmp_app_data], async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.RoleStore
  alias Flezha.Config.Durations
  alias Flezha.Error
  alias Flezha.Relay.{Builder, ClassRegistry, Overwatch}
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.{Cli, HubManager, Relay}
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  @context %{source: :http, principal: "console@_system"}

  setup do
    namespace = "relay_activity_#{System.unique_integer([:positive])}"
    stub(HubManager, :hub_alive?, fn _namespace -> true end)
    %{namespace: namespace, relay: "act_#{System.unique_integer([:positive])}"}
  end

  test "a bound relay is answered with its rows, read time, instance and runner", ctx do
    serve(ctx, bound(ctx.relay))
    me = self()

    expect(ControlUnit, :send_control_await, fn "r1", {:tasklet_activity, command}, opts ->
      send(me, {:asked, command.relay_id, opts[:timeout]})
      {:ok, answer()}
    end)

    assert {:ok, read} = Relay.activity(ctx.namespace, ctx.relay, @context)
    assert_received {:asked, relay, timeout}
    assert relay == ctx.relay
    assert timeout == Durations.get(:tasklet_activity_read_ms)
    assert timeout < Durations.get(:overwatch_call_timeout_ms)

    assert %{relay: ^relay, runner: "r1", read_at_ms: 1234, instance_seq: 7} = read

    assert [
             %{
               tasklet: "ohlc",
               cells: 1,
               queued: 4,
               admitted_total: 100,
               cells_created_total: nil
             },
             %{tasklet: "trades", cells: 2, dropped_capped_total: 3}
           ] = read.tasklets

    assert read.cells == nil, "no Cells asked for, none answered"
  end

  test "a tasklet's Cells are asked for and answered beside the rows", ctx do
    serve(ctx, bound(ctx.relay))
    me = self()

    expect(ControlUnit, :send_control_await, fn "r1", {:tasklet_activity, command}, _opts ->
      send(me, {:asked, command.cells_of, command.limit})
      {:ok, answer(cells())}
    end)

    opts = [cells_of: "trades", limit: 10]
    assert {:ok, read} = Relay.activity(ctx.namespace, ctx.relay, opts, @context)
    assert_received {:asked, "trades", 10}

    assert %{
             tasklet: "trades",
             cells_total: 2,
             rows: [
               %{key: "BTC", queued: 3, admitted_total: 7, last_admitted_ms: 1200},
               %{key: nil, queued: 0, dropped_oldest_total: 1, last_admitted_ms: 900}
             ]
           } = read.cells
  end

  test "a runner that answers no Cells reads as nil, an undeclared tasklet as its refusal",
       ctx do
    serve(ctx, bound(ctx.relay))
    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:ok, answer()} end)

    assert {:ok, %{cells: nil, tasklets: [_, _]}} =
             Relay.activity(ctx.namespace, ctx.relay, [cells_of: "trades"], @context)

    refusal = Error.new("state.tasklet_not_found", relay: ctx.relay, tasklet: "nope")
    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:error, refusal} end)

    assert {:error, ^refusal} =
             Relay.activity(ctx.namespace, ctx.relay, [cells_of: "nope"], @context)
  end

  test "a Cell read the runner could not carry is refused before asking", ctx do
    serve(ctx, bound(ctx.relay))
    stub(ControlUnit, :send_control_await, fn _, _, _ -> flunk("a runner was asked") end)

    assert {:error, %Error{code: "usage.not_text"}} =
             Relay.activity(ctx.namespace, ctx.relay, [cells_of: 5], @context)

    for limit <- [-1, 0x1_0000_0000, "10"] do
      assert {:error, %Error{code: "usage.invalid_number"}} =
               Relay.activity(ctx.namespace, ctx.relay, [cells_of: "t", limit: limit], @context)
    end
  end

  test "a relay that keeps no state is answered as any bound relay", ctx do
    :ok = ClassRegistry.put_persistent(ctx.relay, false)
    on_exit(fn -> ClassRegistry.put_persistent(ctx.relay, true) end)
    serve(ctx, bound(ctx.relay))

    stub(ControlUnit, :send_control_await, fn "r1", {:tasklet_activity, _}, _ ->
      {:ok, answer()}
    end)

    assert {:ok, %{tasklets: [_, _]}} = Relay.activity(ctx.namespace, ctx.relay, @context)
  end

  test "a suspended or unbound relay is refused without asking a runner", ctx do
    stub(ControlUnit, :send_control_await, fn _, _, _ -> flunk("a runner was asked") end)

    for {state, why} <- [
          {%{bound(ctx.relay) | phase: :suspended}, :suspended},
          {%{bound(ctx.relay) | runner_id: nil}, :unbound}
        ] do
      serve(ctx, state)

      assert {:error, %Error{code: "state.not_resident", details: details}} =
               Relay.activity(ctx.namespace, ctx.relay, @context)

      assert details == %{"relay" => ctx.relay, "state" => to_string(why)}
    end
  end

  test "a partition group id is refused as cell_state refuses it", ctx do
    serve(ctx, bound(ctx.relay))
    activity = Relay.activity(ctx.namespace, "demo", @context)

    assert {:error, %Error{code: "relay.not_found"}} = activity
    assert activity == Relay.cell_state(ctx.namespace, "demo", nil, @context)
  end

  test "a runner that does not answer, or does not know the read, is named", ctx do
    serve(ctx, bound(ctx.relay))

    for unanswered <- [
          {:error, :timeout},
          {:error, :no_control_unit},
          {:error, {:control_unit_exit, :noproc}},
          {:ok, ""}
        ] do
      stub(ControlUnit, :send_control_await, fn _, _, _ -> unanswered end)

      assert {:error, %Error{code: "activity.no_answer", details: details}} =
               Relay.activity(ctx.namespace, ctx.relay, @context)

      assert details == %{"relay" => ctx.relay, "runner" => "r1"}
    end
  end

  test "a runner's own refusal is passed through", ctx do
    serve(ctx, bound(ctx.relay))
    refusal = Error.new("runtime.relay_not_found", relay: ctx.relay)
    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:error, refusal} end)

    assert {:error, ^refusal} = Relay.activity(ctx.namespace, ctx.relay, @context)
  end

  test "the Overwatch keeps serving while a read waits on a slow runner", ctx do
    state = bound(ctx.relay)
    me = self()

    stub(ControlUnit, :send_control_await, fn _, _, _ ->
      send(me, {:waiting, self()})

      receive do
        :answer -> {:ok, answer()}
      end
    end)

    ref = make_ref()

    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_tasklet_activity, %{}}, {me, ref}, state)

    assert_receive {:waiting, reader}

    # A second read is taken while the first is still in flight.
    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_tasklet_activity, %{}}, {me, make_ref()}, state)

    assert_receive {:waiting, _second}

    send(reader, :answer)
    assert_receive {^ref, {{:ok, _raw}, "r1"}}
  end

  test "flz relay activity prints one line per tasklet, with - for a figure not kept", ctx do
    serve(ctx, bound(ctx.relay))
    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:ok, answer()} end)

    {_, out, _} =
      CliCapture.run(fn ->
        Cli.call("Relay.activity", Jason.encode!([ctx.namespace, ctx.relay]), [])
      end)

    assert [heading, columns, ohlc, trades] = String.split(out, "\n", trim: true)
    assert heading == "Activity of relay '#{ctx.relay}' on runner 'r1', instance 7:"
    assert String.split(columns) |> hd() == "TASKLET"
    assert String.split(ohlc) == ~w(ohlc 1 4 4 100 0 0 2 - - -)
    assert String.split(trades) == ~w(trades 2 0 0 9 1 0 0 3 5 3)
  end

  test "flz relay activity --cells adds one line per Cell, or says the runner has none", ctx do
    serve(ctx, bound(ctx.relay))
    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:ok, answer(cells())} end)
    args = Jason.encode!([ctx.namespace, ctx.relay, %{cells_of: "trades"}])

    {_, out, _} = CliCapture.run(fn -> Cli.call("Relay.activity", args, []) end)
    assert [_, _, _, _, heading, columns, btc, unkeyed] = String.split(out, "\n", trim: true)
    assert heading == "Cells of tasklet 'trades', 2 of 2, most recent first:"
    assert String.split(columns) == ~w(KEY QUEUED ADMITTED BLOCKED DROP_NEW DROP_OLD AGE_MS)
    assert String.split(btc) == ~w(BTC 3 7 0 0 0 34)
    assert String.split(unkeyed) == ~w(- 0 2 0 0 1 334)

    stub(ControlUnit, :send_control_await, fn _, _, _ -> {:ok, answer()} end)
    {_, out, _} = CliCapture.run(fn -> Cli.call("Relay.activity", args, []) end)

    assert out |> String.split("\n", trim: true) |> List.last() ==
             "Runner 'r1' does not report Cells."
  end

  describe "under enforcement" do
    setup %{namespace: ns} = ctx do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true}])
      :ok = RoleStore.bind("eve", ns, :stopper)
      :ok = RoleStore.bind("vic", ns, :viewer)
      serve(ctx, bound(ctx.relay))
      stub(ControlUnit, :send_control_await, fn _, _, _ -> {:ok, answer()} end)
      :ok
    end

    test "a caller whose role lacks relay_info is refused, and a viewer reads", ctx do
      assert {:error, %Error{code: "authz.role_insufficient"}} =
               Relay.activity(ctx.namespace, ctx.relay, %{source: :cli, principal: "eve"})

      assert {:ok, %{runner: "r1"}} =
               Relay.activity(ctx.namespace, ctx.relay, %{source: :cli, principal: "vic"})
    end
  end

  defp bound(id) do
    %Overwatch{
      id: id,
      namespace: "ns",
      runner_id: "r1",
      builder: %Builder{id: id, factory_id: "f", run: :flow}
    }
  end

  # The relay's Overwatch, answering each call as its `handle_call/3` does.
  defp serve(%{namespace: namespace, relay: relay}, state) do
    pid = spawn_link(fn -> overwatch(state) end)

    stub(HubManager, :get_pid, fn
      ^namespace, ^relay -> {:ok, pid}
      namespace, other -> {:error, {:relay_not_found, namespace, other}}
    end)
  end

  defp overwatch(state) do
    receive do
      {:"$gen_call", from, request} ->
        case Overwatch.handle_call(request, from, state) do
          {:reply, reply, _state} -> GenServer.reply(from, reply)
          {:noreply, _state} -> :ok
        end

        overwatch(state)
    end
  end

  defp cells do
    %RunnerCommands.TaskletCells{
      tasklet: "trades",
      cells_total: 2,
      rows: [
        %RunnerCommands.CellActivityRow{
          key: "BTC",
          queued: 3,
          admitted_total: 7,
          last_admitted_ms: 1200
        },
        %RunnerCommands.CellActivityRow{
          queued: 0,
          admitted_total: 2,
          dropped_oldest_total: 1,
          last_admitted_ms: 900
        }
      ]
    }
  end

  defp answer(cells \\ nil) do
    response = %RunnerCommands.TaskletActivityResponse{
      cells: cells,
      read_at_ms: 1234,
      instance_seq: 7,
      rows: [
        %RunnerCommands.TaskletActivityRow{
          tasklet: "ohlc",
          cells: 1,
          queued: 4,
          queued_max: 4,
          admitted_total: 100,
          blocked_total: 0,
          dropped_newest_total: 0,
          dropped_oldest_total: 2
        },
        %RunnerCommands.TaskletActivityRow{
          tasklet: "trades",
          cells: 2,
          queued: 0,
          queued_max: 0,
          admitted_total: 9,
          blocked_total: 1,
          dropped_newest_total: 0,
          dropped_oldest_total: 0,
          dropped_capped_total: 3,
          cells_created_total: 5,
          cells_evicted_total: 3
        }
      ]
    }

    {:ok, iodata, _size} = Protox.encode(response)
    IO.iodata_to_binary(iodata)
  end
end
