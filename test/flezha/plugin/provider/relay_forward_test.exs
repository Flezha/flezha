defmodule FlezhaTest.Plugin.Provider.RelayForwardTest do
  @moduledoc """
  `relay-to-relay-messaging`, "Effectively-once delivery across the hop for
  durable senders": a payload an effectively-once source emits through the
  relay-forward plugin reaches a durable target once, even when the target's
  confirmation is lost after its journal append and the worker retries.

  The source is the provider's own `on_output/2`; the worker is the real
  `ForwardWorker` on its real delivery path; the target is an Overwatch loop
  around a durable input log, its stream a stand-in that records each forward
  (one forward is one tasklet run).
  """
  # async: false: Mimic global mode; Mimic stubs; the live state tree.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.RelayForward
  alias Flezha.Plugin.Provider.RelayForward.ForwardWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Service.{HubManager, RelayManager}
  alias Flezha.State.{InputLog, Outbox}
  alias FlezhaTest.Helper.OverwatchHost, as: Host

  @ns "default"
  @plugin_id "kb591"
  # A hang guard for a delivery through the worker, not a latency claim.
  @delivery_ms 2_000

  setup :set_mimic_global_until_exit

  setup do
    FlezhaTest.Case.install([:state_tree])
  end

  # The target's stream: every forward reaches the test, as a runner's tasklet
  # run would.
  defp recording_channel do
    test_pid = self()

    stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, id, payload ->
      send(test_pid, {:forwarded, id, payload})
      :ok
    end)

    :fake_channel
  end

  # The first delivery reaches the target and is journaled there, but its
  # confirmation never comes back: the call times out as a slow fsync tick
  # would make it, so the worker retries the same head.
  defp drop_first_confirmation do
    test_pid = self()
    attempts = :counters.new(1, [])

    stub(RelayManager, :send_io_confirming, fn id, ns, message, metadata, timeout ->
      :counters.add(attempts, 1, 1)
      attempt = :counters.get(attempts, 1)

      verdict =
        Mimic.call_original(RelayManager, :send_io_confirming, [
          id,
          ns,
          message,
          metadata,
          timeout
        ])

      send(test_pid, {:attempt, attempt, verdict})

      if attempt == 1,
        do: exit({:timeout, {GenServer, :call, [id, {:ingest, message, metadata}, timeout]}}),
        else: verdict
    end)
  end

  defp start_target(inst, overrides) do
    state = Host.state(inst, [channel: recording_channel()] ++ overrides)
    {:ok, host} = start_supervised({Host, state})
    stub(HubManager, :hub_alive?, fn @ns -> true end)
    stub(HubManager, :get_pid, fn @ns, ^inst -> {:ok, host} end)
    host
  end

  defp start_worker(target, plugin_id \\ @plugin_id) do
    opts = %{
      name: WorkerSup.worker(RelayForward, {{@ns, plugin_id}, target}),
      plugin_id: plugin_id,
      source_namespace: @ns,
      source_relay_id: "source",
      target_relay_id: target,
      queue_max: :infinity,
      retry_backoff_ms: 10,
      retry_max_backoff_ms: 20,
      deliver: RelayForward.deliver(@ns, target, 1_000)
    }

    start_supervised!({ForwardWorker, opts}, id: {ForwardWorker, plugin_id})
  end

  defp forward(inst, metadata, plugin_id \\ @plugin_id) do
    ctx = %Ctx.IO{relay_id: "source", namespace: @ns, message: "tick", metadata: metadata}

    assert {:ok, ^ctx} =
             RelayForward.on_output(ctx, %{
               plugin_id: plugin_id,
               worker_key: {@ns, plugin_id},
               routes: [%{id: inst}]
             })
  end

  defp stamps do
    cell = {"source", "pricer", "EURUSD"}

    Outbox.output_stamps(%{
      cell: cell,
      ack: Outbox.token(cell, {:cursor, 7}),
      seq: "7.0",
      epoch: 1,
      channel: "out",
      key: "EURUSD"
    })
  end

  for {tier, overrides} <- [
        effectively_once: [effectively_once: true],
        durable_steps: [durable_steps: true]
      ] do
    test "a retried delivery is absorbed rather than duplicated (#{tier} target)" do
      inst = "kb591_#{unquote(tier)}_#{System.unique_integer([:positive])}"
      start_target(inst, unquote(overrides))
      start_worker(inst)
      drop_first_confirmation()

      stamps = stamps()
      forward(inst, stamps)

      assert_receive {:attempt, 1, :ok}, @delivery_ms
      assert_receive {:attempt, 2, retry_verdict}, @delivery_ms

      # The stamps survive the hop unmodified.
      assert_receive {:forwarded, ^inst, {"tick", delivered}}, @delivery_ms
      assert Map.take(delivered, Map.keys(stamps)) == stamps

      assert retry_verdict == {:ok, :absorbed}
      assert [{0, "tick", _metadata}] = InputLog.undelivered(inst)
      refute_receive {:forwarded, ^inst, _}, 200
    end
  end

  test "two plugins forwarding one output to one target stay two deliveries" do
    inst = "kb591_two_#{System.unique_integer([:positive])}"
    start_target(inst, effectively_once: true)
    start_worker(inst, "a")
    start_worker(inst, "b")

    forward(inst, stamps(), "a")
    assert_receive {:forwarded, ^inst, {"tick", %{"flz.msg_id" => "a/" <> key}}}, @delivery_ms
    forward(inst, stamps(), "b")
    assert_receive {:forwarded, ^inst, {"tick", %{"flz.msg_id" => "b/" <> ^key}}}, @delivery_ms

    assert key == Outbox.dedup_key(stamps())
    assert [{0, "tick", _}, {1, "tick", _}] = InputLog.undelivered(inst)
  end

  test "a sender's own message id wins, and an output off the tier gets none" do
    inst = "kb591_ids_#{System.unique_integer([:positive])}"
    start_target(inst, effectively_once: true)
    start_worker(inst)

    forward(inst, Map.put(stamps(), "flz.msg_id", "mine"))
    assert_receive {:forwarded, ^inst, {"tick", %{"flz.msg_id" => "mine"}}}, @delivery_ms

    forward(inst, %{"channel" => "out"})
    assert_receive {:forwarded, ^inst, {"tick", off_tier}}, @delivery_ms
    refute Map.has_key?(off_tier, "flz.msg_id")
  end
end
