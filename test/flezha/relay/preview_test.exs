defmodule FlezhaTest.Relay.PreviewTest do
  @moduledoc """
  The Overwatch's `on_preview` hook: a part and a dead-letter notice reach the
  listeners there are, never an output hook or a sink, are never held behind a
  binding, and cost nothing without a listener. The bind that brings up a
  declared plugin its table does not hold is `Flezha.Relay.Plugins.prepare/2`.
  """
  # async: false: Mimic global mode.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Relay.Plugins
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Table
  alias FlezhaTest.Helper.OverwatchHost, as: Host

  setup :set_mimic_global_until_exit

  setup do
    %{inst: "preview_#{System.unique_integer([:positive])}"}
  end

  defp listener(inst, id) do
    Plugin.new(
      id: id,
      provider: Plugin.Provider.MonitoredIOForward,
      opts: %{recv_pid: self(), plugin_id: id, relay_id: inst, namespace: "default"},
      namespace: "default",
      relay_id: inst,
      hooks: [{:on_output, []}, {:on_preview, []}]
    )
  end

  # A sink stand-in: every frame its `on_output` is given comes to the test.
  defp sink(inst) do
    Plugin.new(
      id: "sink",
      provider: Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      namespace: "default",
      relay_id: inst,
      hooks: [{:on_output, []}]
    )
  end

  defp host(inst, plugins, overrides \\ []) do
    state = Host.state(inst, overrides)
    {:ok, table} = Table.bulk_subscribe(state.extensions, plugins)
    start_supervised!({Host, %{state | extensions: table}})
  end

  @part {"hel", %{"channel" => "events", "flz.part" => "0", "flz.part_attempt" => "1"}}

  test "a part reaches the listener's preview and never a sink", %{inst: inst} do
    host = host(inst, [{"sink", sink(inst)}, {"l1", listener(inst, "l1")}])
    {message, metadata} = @part

    GenServer.cast(host, {:io_part, message, metadata})
    GenServer.cast(host, {:io_out, "hello", %{"channel" => "events"}})

    assert_receive {:preview_forward, {"hel", ^metadata}}
    assert_receive {:message_forward, {"hello", _}}
    assert_receive {:message_forward, "hello"}
    refute_received {:message_forward, "hel"}
    refute_received {:message_forward, {"hel", _}}
  end

  test "two listeners both see a part", %{inst: inst} do
    host = host(inst, [{"l1", listener(inst, "l1")}, {"l2", listener(inst, "l2")}])
    {message, metadata} = @part
    GenServer.cast(host, {:io_part, message, metadata})

    assert_receive {:preview_forward, {"hel", _}}
    assert_receive {:preview_forward, {"hel", _}}
  end

  test "with no listener a part builds no context and calls no plugin", %{inst: inst} do
    host = host(inst, [{"sink", sink(inst)}])
    reject(Extension, :run, 3)
    {message, metadata} = @part

    GenServer.cast(host, {:io_part, message, metadata})
    _ = :sys.get_state(host)
    refute_received {:message_forward, _}
  end

  test "a part is never held behind a binding in flight", %{inst: inst} do
    host = host(inst, [{"l1", listener(inst, "l1")}], phase: :suspended)
    :sys.replace_state(host, &%{&1 | bind_task: %{ref: make_ref(), pid: self()}})
    {message, metadata} = @part

    GenServer.cast(host, {:io_part, message, metadata})
    GenServer.cast(host, {:io_out, "held", %{}})

    assert_receive {:preview_forward, {"hel", _}}
    assert [{:io_out, "held", _}] = :sys.get_state(host).held_outputs
  end

  test "a dead-letter notice reaches the listener as the input.dead_lettered document",
       %{inst: inst} do
    host = host(inst, [{"sink", sink(inst)}, {"l1", listener(inst, "l1")}])

    notice = %{
      token: nil,
      entry: "#{inst}/t/1",
      error: "boom",
      channel: "in",
      offset: 7,
      tasklet: "t",
      attempts: 3,
      new: true
    }

    # An entry already parked is told to no listener.
    GenServer.cast(host, {:dead_lettered, %{notice | offset: 6, new: false}})
    GenServer.cast(host, {:dead_lettered, notice})

    assert_receive {:preview_forward, {document, %{"channel" => "in", "flz.offset" => "7"}}}
    refute_received {:preview_forward, {_, %{"flz.offset" => "6"}}}
    error = Flezha.SDK.Error.from_document(document)
    assert error.code == "input.dead_lettered"
    assert error.details["entry"] == "#{inst}/t/1"
    assert error.details["error"] == "boom"
    assert error.hint == "flz dlq show #{inst}/t/1"
    refute_received {:message_forward, _}
  end

  describe "prepare/2" do
    test "subscribes only the declared plugins its table does not hold", %{inst: inst} do
      state = Host.state(inst)
      {:ok, table} = Table.bulk_subscribe(state.extensions, [{"l1", listener(inst, "l1")}])
      state = %{state | extensions: table}
      declared = [{"l1", listener(inst, "l1")}, sink(inst)]

      assert {:ok, prepared} = Plugins.prepare(state, declared)
      assert Table.phase(prepared.extensions, "sink") != :unknown
      assert prepared.extensions.entries["l1"] == table.entries["l1"]

      # An ordinary rebind finds every declared id on the table and touches none.
      assert {:ok, again} = Plugins.prepare(prepared, declared)
      assert again.extensions.entries == prepared.extensions.entries
    end
  end
end
