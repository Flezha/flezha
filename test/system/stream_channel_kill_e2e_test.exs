defmodule FlezhaTest.System.StreamChannelKillE2ETest do
  @moduledoc """
  A relay keeps every input it acknowledged when the data channel carrying
  its inputs is killed under load.

  The channel comes back unbound under its supervisor. The runner must be
  asked for fresh streams within seconds, and what the dead stream swallowed
  must reach the runner again from the input log, so the Cell ends holding
  every input its senders were told was taken. That none counts twice is
  N-22's, a property of the recovery every lost stream shares.

  Tagged `:system` (boots a real subprocess). Run with
  `mix test --include system test/system/stream_channel_kill_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :dynamic_cell

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  require Flezha.Relay.Ingest.Verdict

  alias Flezha.Payloader
  alias Flezha.Relay.Builder
  alias Flezha.Relay.Ingest.Verdict
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.TierE2EHarness

  @namespace "stream_channel_kill"
  @factory "dynamic_cell"
  @tasklet "dynamic_cell_tasklet"
  @key "k1"
  @inputs 400
  @rebind_ms 10_000
  @test_timeout 120_000

  setup do
    on_exit(fn -> TierE2EHarness.stop_namespace(@namespace) end)
  end

  # fails when: the killed channel's replacement stays unbound while the
  # runner stays listed active, and the relay keeps answering `:ok` for inputs
  # it never counts (N-13).
  @tag reference_runtime: true
  @tag timeout: @test_timeout
  test "a killed bound channel is rebound and the relay counts every acknowledged input" do
    {_port, _runner_id} = TierE2EHarness.boot_runner(@namespace, TierE2EHarness.unique_id_path())

    relay_id = "channel_kill_#{System.unique_integer([:positive])}"
    builder = %Builder{id: relay_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, _info} = RelayManager.start_relay(@namespace, builder)

    {:ok, %{channel: channel}} = RelayManager.get_overwatch_state(@namespace, relay_id)
    assert wait_until(fn -> rebound?(channel, nil) end, @rebind_ms)

    sender = Task.async(fn -> send_counting(relay_id, 1..@inputs) end)
    assert wait_until(fn -> seen(relay_id) >= div(@inputs, 4) end, 30_000)

    killed = GenServer.whereis(channel)
    Process.exit(killed, :kill)

    assert wait_until(fn -> rebound?(channel, killed) end, @rebind_ms),
           "the channel's replacement was not bound again within #{@rebind_ms} ms"

    acked = Task.await(sender, @test_timeout)

    assert wait_until(fn -> taken_all?(relay_id, acked) end, 60_000),
           "acknowledged #{inspect(acked)}, counted #{inspect(Enum.frequencies(seen_inputs(relay_id)))}"
  end

  # Every input is sent through the confirming door, one at a time; the answer
  # is the inputs the relay said it took.
  defp send_counting(relay_id, range) do
    Enum.filter(range, fn n ->
      verdict = RelayManager.send_io_confirming(relay_id, @namespace, "#{@key}:#{n}", %{})
      Verdict.taken?(verdict)
    end)
  end

  # Every acknowledged input reached the Cell at least once.
  defp taken_all?(relay_id, acked) do
    seen = MapSet.new(seen_inputs(relay_id))
    Enum.all?(acked, &MapSet.member?(seen, "#{@key}:#{&1}"))
  end

  defp rebound?(channel, killed) do
    case GenServer.whereis(channel) do
      pid when is_pid(pid) and pid != killed ->
        GenServer.call(channel, :stream_ref_status) != nil

      _ ->
        false
    end
  end

  defp seen(relay_id), do: length(seen_inputs(relay_id))

  defp seen_inputs(relay_id) do
    query = %{tasklet: @tasklet, cell_key: @key}

    with {:ok, json, _seq} <- RelayManager.get_cell_state(@namespace, relay_id, query),
         %{@tasklet => %{@key => %{"seen" => seen}}} <- Payloader.json_to_map(json) do
      seen
    else
      _ -> []
    end
  end
end
