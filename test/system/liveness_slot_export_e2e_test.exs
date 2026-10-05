defmodule FlezhaTest.System.LivenessSlotExportE2ETest do
  @moduledoc """
  Every liveness slot is declared, and every declaration reaches the surface it
  names.

  The scatter this closes is a counted fact restated in five places: the slot
  layout, the exporter's row list, the mirror's key list, the SDK's heartbeat
  map and two documents. Three defects came out of it, each closed at one site
  (`report-parked-relay-visibility`, `count-a-runner-self-report-once`,
  KI-192). This scenario is the end-to-end half of the guard: a real runner on
  a real transport, its heartbeat mirrored into real slots, and the declared
  metrics scraped out of a real `/metrics` body.

  `@producers` names, per slot, the action that makes it move — here, or in the
  scenario that already drives it. A slot with no entry FAILS, so a row cannot
  be added to the layout and quietly left unproven.

  Tagged `:system` (boots real subprocesses); run with
  `mix test --include system test/system/liveness_slot_export_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :liveness

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 3, produces?: 5, unique_id_path: 1]

  alias Flezha.Observability.Export.{PrometheusHttp, PrometheusRegistry, PrometheusSink}
  alias Flezha.Observability.OtpBackend.TransportSource
  alias Flezha.Observability.TestSinks
  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Fixtures.RelayFixture

  @factory "wedge_probe"
  @heartbeat_ms 200

  # One entry per declared slot: `:here` is driven by this scenario, and a path
  # names the scenario that already drives it. Both are checked — a path that
  # does not exist fails, so an entry cannot rot into a promise.
  @producers %{
    # Driven here: a bound runner, an input, an output and its heartbeats.
    last_control_frame_ms: :here,
    io_in_received: :here,
    send_frame_calls: :here,
    flz_data_received: :here,
    last_send_ms: :here,
    last_heartbeat_ms: :here,
    hb_count: :here,
    hb_dispatches: :here,
    hb_inflight: :here,
    hb_inflight_oldest_ms: :here,
    forwarded: :here,
    # Driven by the scenario that owns the mechanism.
    nil_drops: "test/flezha/relay/ingest_test.exs",
    filtered_nil: "test/flezha/relay/ingest_test.exs",
    parked_drops: "test/flezha/relay/ingest_test.exs",
    hb_credit_parked: "test/system/credit_flow_e2e_test.exs",
    credit_active: "test/system/credit_flow_e2e_test.exs",
    credit_ledger_level: "test/system/credit_flow_e2e_test.exs",
    credit_overrun_total: "test/flezha/runner/stream_channel_credit_test.exs",
    hb_blocked_parks: "test/sdk_conformance/gate_test.exs",
    hb_drops_newest: "test/sdk_conformance/gate_test.exs",
    hb_drops_oldest: "test/sdk_conformance/gate_test.exs",
    hb_pending_pushes: "test/sdk_conformance/gate_test.exs",
    hb_pending_push_oldest_ms: "test/sdk_conformance/gate_test.exs",
    hb_restore_discarded_inputs: "test/flezha/sdk/effectively_once_test.exs",
    hb_timers_armed: "test/system/cell_timers_e2e_test.exs",
    hb_timers_fired: "test/system/cell_timers_e2e_test.exs",
    hb_parts_sent: "test/system/part_stream_e2e_test.exs",
    # The runner's own counters, which these mirror, are moved by its unit suite.
    hb_parts_dropped_no_stream: "flz_core/src/part_tests.rs",
    hb_parts_dropped_refused: "flz_core/src/part_tests.rs",
    hb_parts_dropped_too_large: "flz_core/src/part_tests.rs",
    stale_writer_refused: "test/flezha/state/runner_restart_realign_test.exs",
    send_gate_held: "test/flezha/runner/stream_channel_gate_test.exs",
    foreign_frame_refused: "test/system/runner_ownership_e2e_test.exs"
  }

  setup do
    namespace = "liveness_export_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    %{namespace: namespace}
  end

  # This one needs no fleet: it asks the declaration about itself, and it is
  # what stops a new row being added with nothing behind it.
  test "every declared slot names the action that moves it" do
    slots =
      Enum.map(
        LivenessCounters.channel_rows() ++ LivenessCounters.relay_rows(),
        &Keyword.fetch!(&1, :name)
      )

    missing = Enum.reject(slots, &Map.has_key?(@producers, &1))

    assert missing == [],
           "liveness slots with no producer named: #{inspect(missing)} — a slot is declared " <>
             "with the action that makes it move, here or in the scenario that drives it"

    for {slot, path} <- @producers, is_binary(path) do
      assert File.exists?(path),
             "#{slot} names #{path} as the scenario that drives it, and that file is gone"
    end

    stale = Enum.reject(Map.keys(@producers), &(&1 in slots))
    assert stale == [], "producers named for slots the layout no longer has: #{inspect(stale)}"
  end

  @tag timeout: 180_000
  test "a real runner's counted facts reach the surfaces their rows declare",
       %{namespace: namespace} do
    start_supervised!(PrometheusRegistry)
    start_supervised!(PrometheusSink)

    original_export = Application.get_env(:flezha, :observability_export, [])
    on_exit(fn -> Application.put_env(:flezha, :observability_export, original_export) end)

    Application.put_env(:flezha, :observability_export,
      prometheus_enabled: true,
      prometheus_port: 0,
      prometheus_bind: "127.0.0.1"
    )

    start_supervised!(PrometheusHttp)
    {:ok, port} = PrometheusHttp.listen_port()

    relay_id = "liveness_#{System.unique_integer([:positive])}"

    {_port, runner_id} =
      boot_runner(namespace, unique_id_path("liveness_e2e"),
        env: [{"FLEZHA_DATA_HEARTBEAT_MS", Integer.to_string(@heartbeat_ms)}]
      )

    assert {:ok, %{runner_id: ^runner_id}} =
             RelayManager.start_relay(namespace, builder(namespace, relay_id, self()))

    assert produces?(relay_id, namespace, "one", "ok:one", 60_000),
           "the relay never produced, so nothing counted"

    # The slots hold the zero a runner starts at until its first heartbeat
    # lands, so wait for a real self-report rather than for a number of
    # heartbeat intervals to pass: on a loaded box five intervals of wall clock
    # do not buy five heartbeats.
    channels =
      wait_until(
        fn ->
          channels = LivenessCounters.snapshot_all().channels

          case channels |> LivenessCounters.by_runner() |> Map.get(runner_id) do
            %{hb_count: count, hb_dispatches: dispatches} when count > 0 and dispatches >= 1 ->
              channels

            _ ->
              nil
          end
        end,
        30_000
      )

    assert channels, "no heartbeat self-report reached the runner's slots"
    row = channels |> LivenessCounters.by_runner() |> Map.fetch!(runner_id)

    # The layout is what the channel registered: every declared slot is
    # readable, with no second list saying which ones exist.
    for slot <- LivenessCounters.channel_slots() do
      assert is_integer(Map.get(row, slot)),
             "#{slot} is declared and the snapshot does not carry it"
    end

    assert row.hb_count > 0, "no heartbeat reached the controller"
    assert row.io_in_received > 0, "the channel counted no inbound frame"

    # The two stamps are written by different channels of one runner: the
    # control channel writes one, the data channels the other, and a monotonic
    # reading is negative on the BEAM. So each is asserted twice — on the
    # channel that wrote it, and on the collapsed row a reader actually takes
    # it from — which tells a collapse that dropped a stamp apart from a stamp
    # nobody took.
    control = Enum.find(channels, &(&1.runner_id == runner_id and &1.name == :control))
    assert control, "the control channel registered no counters"
    assert control.last_control_frame_ms != 0, "the control stamp was never written"

    secondary = Enum.filter(channels, &(&1.runner_id == runner_id and &1.name != :control))
    sends = secondary |> Enum.map(& &1.last_send_ms) |> Enum.reject(&(&1 == 0))

    assert sends != [], "no data channel stamped its last send"

    assert row.last_control_frame_ms == control.last_control_frame_ms,
           "the collapse lost the control stamp"

    assert row.last_send_ms == Enum.max(sends), "the collapse lost the data stamp"

    # And the reader that exists for them: a live runner reports a real silence
    # on both halves of the session row, never "not measured".
    liveness =
      LivenessCounters.runner_liveness(channels, nil, System.monotonic_time(:millisecond))

    assert %{control_silent_ms: control_ms, data_silent_ms: data_ms} = liveness[runner_id]
    assert is_integer(control_ms), "control_silent_ms read as not measured"
    assert is_integer(data_ms), "data_silent_ms read as not measured"

    # A per-runner fact read once per runner, not once per channel: the defect
    # `count-a-runner-self-report-once` closed, asserted through a real fleet.
    assert row.hb_dispatches >= 1, "the runner reported no completed dispatch"

    # Every metric row the layout declares reaches a real scrape body.
    TransportSource.start_link([]) |> drive_one_poll()
    TestSinks.settle(PrometheusSink)
    body = scrape(port)

    for {_group, _row, name} <- LivenessCounters.surfaces(:metric) do
      assert body =~ "flezha_transport_#{name}",
             "#{name} is declared a metric and never reached the /metrics body"
    end

    # And the wire contract itself: the mirror's key list and the runner's are
    # the same list, because there is only one.
    assert length(LivenessCounters.heartbeat_keys()) == 16
  end

  # ---------- helpers ----------

  defp drive_one_poll({:ok, pid}) do
    send(pid, :poll)
    _ = :sys.get_state(pid)
    :ok = GenServer.stop(pid)
  end

  defp scrape(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, packet: :raw, active: false])

    :ok =
      :gen_tcp.send(
        socket,
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
      )

    body = recv_all(socket, "")
    :gen_tcp.close(socket)

    case String.split(body, "\r\n\r\n", parts: 2) do
      [_headers, rest] -> rest
      _ -> body
    end
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, chunk} -> recv_all(socket, acc <> chunk)
      {:error, :closed} -> acc
      {:error, _} -> acc
    end
  end

  defp builder(namespace, relay_id, recv_pid) do
    sink = %Flezha.Plugin{
      id: "liveness_sink",
      namespace: nil,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: recv_pid, include_metadata: true},
      hooks: [{:on_output, []}]
    }

    %Builder{
      id: relay_id,
      factory_id: @factory,
      init_values: %{},
      opts: [bindings: RelayFixture.bindings(namespace, relay_id, [sink])]
    }
  end
end
