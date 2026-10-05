defmodule Flezha.Service.RelayCallTest do
  @moduledoc """
  `relay-calls` from outside Flezha: the published
  `Flezha.Service.Relay.call/6`, the `flz relay call` verb and `send
  --key`, and the one member resolution a partition group is reached through.
  The callee's Overwatch is a stand-in answering the `{:call, ...}` it is sent,
  so these pin the resolution, the authorization and the rendering; the table
  behind a real Overwatch is `FlezhaTest.Relay.CallsTest`'s.
  """
  # async: false: Mimic global mode; the auth stores and the audit sink are singletons.
  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.AuthZ.{RoleStore, Verbs}
  alias Flezha.Error
  alias Flezha.Observability.{RingBufferSink, TestSinks}
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Service.{Cli, HubManager, Relay, RelayManager}
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

  @context %{source: :ui, principal: "console@_system"}

  setup do
    test_pid = self()
    members = for index <- 0..1, do: member("prices", index)

    # Stands in for each running relay's Overwatch: answers a call with its
    # own payload, prefixed by the relay it reached.
    callee = fn id ->
      spawn_link(fn ->
        receive do
          {:"$gen_call", from, {:call, message, metadata, deadline_ms}} ->
            send(test_pid, {:called, id, message, metadata, deadline_ms})
            GenServer.reply(from, {:ok, "#{id}:#{message}", %{"channel" => "out"}})
        end
      end)
    end

    running = ["echo" | Enum.map(members, & &1.id)]
    stub(HubManager, :hub_alive?, fn _namespace -> true end)

    stub(HubManager, :get_pid, fn _namespace, id ->
      if id in running, do: {:ok, callee.(id)}, else: {:error, "no pid found"}
    end)

    stub(PartitionGroup, :members, fn
      _namespace, "prices" -> members
      _namespace, _id -> []
    end)

    stub(RelayManager, :send_io_confirming, fn id, _namespace, message, _metadata ->
      send(test_pid, {:sent, id, message})
      :ok
    end)

    %{
      owned_by_p1:
        Enum.find(~w(EURUSD GBPUSD USDJPY AUDUSD USDCAD NZDUSD), &owns?(members, 1, &1))
    }
  end

  defp member(group, index) do
    %{
      id: Partition.member_id(group, index),
      partition: %{range: Enum.at(Partition.ranges(2), index)}
    }
  end

  defp owns?(members, index, key),
    do: Partition.owns?(Enum.at(members, index).partition.range, key)

  describe "resolve_member/3" do
    test "names the relay itself, a group's owning member, or the group's members",
         %{owned_by_p1: key} do
      assert {:ok, "echo"} = RelayManager.resolve_member("default", "echo", nil)
      assert {:ok, "prices#p1"} = RelayManager.resolve_member("default", "prices", key)

      assert {:error, {:unkeyed, "prices", ["prices#p0", "prices#p1"]}} =
               RelayManager.resolve_member("default", "prices", nil)

      assert {:error, {:relay_not_found, "default", "ghost"}} =
               RelayManager.resolve_member("default", "ghost", "k")
    end
  end

  describe "Relay.call/6" do
    test "answers with the answer's payload and the channel that produced it" do
      assert {:ok, "echo:hello", %{"channel" => "out"}} =
               Relay.call("default", "echo", "hello", "in", [deadline_ms: 2_000], @context)

      assert_received {:called, "echo", "hello", %{"channel" => "in"}, 2_000}
    end

    test "reaches a group's member by key, and names the members without one",
         %{owned_by_p1: key} do
      assert {:ok, "prices#p1:x", _} =
               Relay.call("default", "prices", "x", "", [deadline_ms: 100, key: key], @context)

      assert_received {:called, "prices#p1", "x", _, 100}

      assert {:error, %Error{code: "call.target_unkeyed", class: :invalid} = error} =
               Relay.call("default", "prices", "x", "", [deadline_ms: 100], @context)

      assert error.message =~ "prices#p0, prices#p1"
      refute_received {:called, _, _, _, _}
    end

    test "a relay that is not running is not found" do
      assert {:error, %Error{code: "relay.not_found"}} =
               Relay.call("default", "ghost", "x", "", [deadline_ms: 100], @context)
    end

    test "writes the record a send writes, naming the relay and a failure's code" do
      boundary = System.monotonic_time(:microsecond)
      {:ok, _, _} = Relay.call("default", "echo", "x", "", [deadline_ms: 100], @context)
      {:error, _} = Relay.call("default", "ghost", "x", "", [deadline_ms: 100], @context)
      TestSinks.settle(RingBufferSink)

      assert [answered, refused] =
               :relay_service
               |> RingBufferSink.query(boundary)
               |> Enum.filter(&(&1.name == :send))

      assert %{action: :"relay.send", relay: "echo", outcome: :ok} = answered.metadata

      assert %{relay: "ghost", outcome: :error, error_code: "relay.not_found"} =
               refused.metadata
    end
  end

  describe "under enforcement" do
    setup do
      FlezhaTest.Case.install([{:auth_stores, enabled: true, claim: true}])
      :ok = RoleStore.bind("eve", "default", :viewer)
      :ok
    end

    test "a viewer is refused as relay.send and delivers nothing" do
      assert {:error, %Error{code: "authz.role_insufficient"}} =
               Relay.call("default", "echo", "x", "", [deadline_ms: 100], %{
                 source: :cli,
                 principal: "eve"
               })

      assert Verbs.fetch!(:relay_call).action == :"relay.send"
      refute_received {:called, _, _, _, _}
    end
  end

  describe "flz relay call" do
    test "prints the answer's payload and one newline" do
      assert {_outcome, "echo:hello\n", ""} =
               CliCapture.run(fn ->
                 Cli.call(
                   "Relay.call",
                   Jason.encode!(["default", "echo", "hello", "", %{deadline_ms: 30_000}]),
                   []
                 )
               end)

      assert_received {:called, "echo", "hello", %{}, 30_000}
    end

    test "a group without a key fails naming its members; a missing relay is not found" do
      {_outcome, out, err} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.call",
            Jason.encode!(["default", "prices", "x", "", %{deadline_ms: 100}]),
            []
          )
        end)

      assert out == ""
      assert err =~ "(call.target_unkeyed)"
      assert err =~ "prices#p0, prices#p1"

      {_outcome, _out, err} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.call",
            Jason.encode!(["default", "ghost", "x", "", %{deadline_ms: 100}]),
            []
          )
        end)

      assert err =~ "(relay.not_found)"
    end

    test "a deadline that is not positive is refused before anything is sent" do
      {_outcome, _out, err} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.call",
            Jason.encode!(["default", "echo", "x", "", %{deadline_ms: 0}]),
            []
          )
        end)

      assert err =~ "(usage.invalid_number)"
      refute_received {:called, _, _, _, _}
    end
  end

  describe "flz relay send" do
    test "reaches a group's owning member by key", %{owned_by_p1: key} do
      {_outcome, out, ""} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.send",
            Jason.encode!(["default", "prices", "x", "", %{key: key}]),
            []
          )
        end)

      assert out =~ "Delivered to 'prices#p1'"
      assert_received {:sent, "prices#p1", "x"}
    end

    test "a message id rides beside the key as flz.msg_id", %{owned_by_p1: key} do
      test_pid = self()

      stub(RelayManager, :send_io_confirming, fn id, _namespace, _message, metadata ->
        send(test_pid, {:sent_with, id, metadata})
        {:ok, :absorbed}
      end)

      {_outcome, out, ""} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.send",
            Jason.encode!(["default", "prices", "x", "in", %{key: key, msg_id: "fx/0/7"}]),
            []
          )
        end)

      assert out =~ "Absorbed (duplicate)"
      assert_received {:sent_with, "prices#p1", %{"channel" => "in", "flz.msg_id" => "fx/0/7"}}
    end

    test "a message id that is not text is refused before anything is sent" do
      {_outcome, _out, err} =
        CliCapture.run(fn ->
          Cli.call(
            "Relay.send",
            Jason.encode!(["default", "echo", "x", "", %{msg_id: 41}]),
            []
          )
        end)

      assert err =~ "(usage.not_text)"
      refute_received {:sent, _, _}
    end

    test "a group without a key names its members and is never 'not found'" do
      {_outcome, _out, err} =
        CliCapture.run(fn ->
          Cli.call("Relay.send", Jason.encode!(["default", "prices", "x", "", %{}]), [])
        end)

      assert err =~ "(call.target_unkeyed)"
      assert err =~ "prices#p0, prices#p1"
      refute err =~ "not running"
      refute_received {:sent, _, _}
    end
  end
end
