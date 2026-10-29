defmodule FlezhaTest.System.PartStreamE2ETest do
  @moduledoc """
  Parts end to end on the runtime `FLEZHA_E2E_RUNTIME` selects: the `part_echo`
  (flow) and `part_echo_steps` (`per_input`) fixtures send the parts
  `1..n` for the input `n` and then emit `done:<n>` on their sink `answer`, and
  `retry:<n>` sends them again on a second attempt.

  Read through `flz relay listen` (text and JSON), through
  `Flezha.Service.Listener.listen/4` from this process as a plugin would, and
  beside a file sink that must hold the output alone. A dead-lettered input
  reaches a listener as a notice.

      FLEZHA_E2E_RUNTIME=python mix test --include system test/system/part_stream_e2e_test.exs
  """

  use FlezhaTest.SystemCase, capability: :parts

  import ExUnit.CaptureIO
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness, only: [runner_namespace: 2, unique: 1]
  import FlezhaTest.Helper.ListenerHarness

  alias Flezha.Relay.Builder
  alias Flezha.Service.{Cli, Listener, RelayManager}
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Fixtures.RelayFixture

  @moduletag timeout: 300_000

  @fixtures [part_echo: :flow, part_echo_steps: :steps]

  setup do
    # A short data heartbeat, so the counts it carries arrive within a test.
    {:ok, namespace: runner_namespace("parts", env: [{"FLEZHA_DATA_HEARTBEAT_MS", "200"}])}
  end

  for {factory, kind} <- @fixtures do
    describe "#{factory}" do
      test "flz relay listen prints the parts, then the output, in text and in JSON",
           %{namespace: ns} do
        id = start!(ns, unquote(factory), unquote(kind))

        text = cli_listen(ns, id, "text", fn -> send!(ns, id, "3") end)

        # A durable steps output carries its stamps as a third field.
        assert [p1, p2, p3, output | _] = text
        assert p1 == "answer\t1\tpart=0 attempt=1 cell=#{cell(id)}"
        assert p2 == "answer\t2\tpart=1 attempt=1 cell=#{cell(id)}"
        assert p3 == "answer\t3\tpart=2 attempt=1 cell=#{cell(id)}"
        assert output =~ ~r/^answer\tdone:3(\t|$)/

        json = cli_listen(ns, id, "json", fn -> send!(ns, id, "2") end)
        lines = Enum.map(json, &Jason.decode!/1)

        assert [
                 %{"kind" => "part", "payload" => "1"},
                 %{"kind" => "part", "payload" => "2"},
                 %{"kind" => "output", "payload" => "done:2"} | _
               ] = lines
      end

      test "a plugin's listener sees the parts, and a file sink the output alone",
           %{namespace: ns} do
        path = sink_path("part_sink")
        id = start!(ns, unquote(factory), unquote(kind), [file_sink("part_sink", path)])
        test = self()

        outcome =
          Listener.listen(ns, id, [window_ms: 60_000, before: fn -> send!(ns, id, "3") end], fn
            {:output, "done:3", _} = event ->
              send(test, event)
              :done

            event ->
              send(test, event)
              :continue
          end)

        assert outcome == {:ok, :done}

        for {payload, n} <- [{"1", "0"}, {"2", "1"}, {"3", "2"}] do
          assert_received {:part, ^payload, %{"flz.part" => ^n, "channel" => "answer"}}
        end

        assert wait_until(fn -> sink_lines(path) == ["done:3"] end, 30_000),
               "the file holds #{inspect(sink_lines(path))}"

        assert wait_until(fn -> listeners(ns, id) == [] end, 30_000)
        # The runner reports what it sent on its next data heartbeat.
        assert wait_until(fn -> hb_parts_sent() > 0 end, 30_000)
      end

      test "a retried attempt's parts start again from 0 with attempt 2", %{namespace: ns} do
        id = start!(ns, unquote(factory), unquote(kind))
        test = self()

        {:ok, :done} =
          Listener.listen(
            ns,
            id,
            [window_ms: 60_000, before: fn -> send!(ns, id, "retry:2") end],
            fn
              {:output, _, _} -> :done
              {:part, payload, md} -> send(test, {:part, payload, md}) && :continue
            end
          )

        stamps =
          for _ <- 1..4 do
            assert_received {:part, payload, md}
            {payload, md["flz.part"], md["flz.part_attempt"]}
          end

        assert stamps == [{"1", "0", "1"}, {"2", "1", "1"}, {"1", "0", "2"}, {"2", "1", "2"}]
      end
    end
  end

  test "a dead-lettered input of a flow relay is printed as a notice",
       %{namespace: ns} do
    id = start!(ns, :poison_probe, :flow)
    text = cli_listen(ns, id, "text", fn -> send!(ns, id, "poison") end, &(&1 =~ "error="))

    assert [notice] = Enum.filter(text, &(&1 =~ "error="))
    assert [channel, _message, fields] = String.split(notice, "\t")
    assert channel == "input"
    assert fields =~ ~r/^error=input\.dead_lettered entry=#{id}\/\S+$/
  end

  test "idle events stay away while a steps session's turn streams parts",
       %{namespace: ns} do
    id = start!(ns, :part_echo_steps, :steps)
    test = self()

    {:ok, :done} =
      Listener.listen(
        ns,
        id,
        [window_ms: 60_000, idle_ms: 500, before: fn -> send!(ns, id, "20") end],
        fn
          :idle -> send(test, :idle) && :continue
          {:part, _, _} -> send(test, :part) && :continue
          {:output, "done:20", _} -> :done
        end
      )

    # The turn streams from its first part on; before it, the pass is still
    # being placed, which an idle event may report.
    assert [:part | streaming] = Enum.drop_while(heard([]), &(&1 == :idle))
    refute :idle in streaming
  end

  defp heard(events) do
    receive do
      event when event in [:idle, :part] -> heard([event | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp start!(ns, factory, kind, plugins \\ []) do
    id = unique(to_string(factory))

    builder = %Builder{
      id: id,
      factory_id: to_string(factory),
      run: kind,
      opts: [bindings: RelayFixture.bindings(ns, id, plugins)]
    }

    assert {:ok, _} = RelayManager.start_relay(ns, builder)
    id
  end

  # `:ok`, which a listener's `before` must answer to go on listening.
  defp send!(ns, id, text) do
    assert capture_io(fn -> Cli.call("Relay.send", Jason.encode!([ns, id, text, ""]), []) end) =~
             "to '#{id}'"

    :ok
  end

  # `flz relay listen` in a process of its own whose group leader collects
  # what it prints, until a line `done?` accepts (an output by default).
  defp cli_listen(ns, id, format, before, done? \\ &String.contains?(&1, "done:")) do
    {:ok, device} = StringIO.open("")

    {pid, ref} =
      spawn_monitor(fn ->
        Process.group_leader(self(), device)

        Cli.call(
          "Relay.listen",
          Jason.encode!([ns, id, "output", %{"window_ms" => 60_000}]),
          output: format
        )
      end)

    assert wait_until(fn -> listeners(ns, id) != [] end, 30_000)
    before.()
    assert wait_until(fn -> Enum.any?(printed(device), done?) end, 60_000)
    text = printed(device)
    # `flz` going away ends the listen, which detaches as it returns.
    Process.exit(device, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 30_000
    assert wait_until(fn -> listeners(ns, id) == [] end, 30_000)
    text
  end

  defp printed(device),
    do: device |> StringIO.contents() |> elem(1) |> String.split("\n", trim: true)

  defp cell(id), do: "#{id}/#{tasklet(id)}/"

  # The fixtures' tasklet names differ by kind; the part names its own Cell.
  defp tasklet(id),
    do: if(String.starts_with?(id, "part_echo_steps"), do: "part_echo_turn", else: "part_echo")

  defp hb_parts_sent do
    LivenessCounters.snapshot_all().channels
    |> LivenessCounters.by_runner()
    |> Map.values()
    |> Enum.map(&Map.get(&1, :hb_parts_sent, 0))
    |> Enum.sum()
  end
end
