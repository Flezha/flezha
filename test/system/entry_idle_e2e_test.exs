defmodule FlezhaTest.System.EntryIdleE2ETest do
  @moduledoc """
  End-to-end proof for `entry_idle` against a live runner: the SDK reports the
  silence of an entry channel as episodes, and reports it once per episode.

  The signal is the SDK's — only it knows a frame arrived — and the policy is
  the application's, so what is proved here is the SDK half: absent before the
  first frame, no episode for a cold start, one `idle` when the gap first
  exceeds the threshold, one `resumed` when a frame ends it, and no second
  `idle` while the silence continues.

  An `IdleObserver` takes no ctx and cannot emit, so the fixture makes the
  callback observable: the observer counts episodes into process globals and a
  procedure reports them as `<absent_at_boot>:<episodes>:<resumes>`.

  The fixture declares TWO entry channels fanning into one procedure, and that
  is what lets this test poll
  without a sleep. The core interns one observation cell per channel NAME, so a
  frame on `idle_report` runs the reporting procedure while leaving
  `idle_input`'s stamp untouched — the silence being measured survives every
  poll that measures it.

  Run with `mix test --include system test/system/entry_idle_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :entry_idle

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]

  import FlezhaTest.Helper.TierE2EHarness,
    only: [boot_runner: 2, cleanup: 3, unique: 1, unique_id_path: 0]

  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.WorkflowResumeHarness

  @moduletag timeout: 300_000

  # The fixture's threshold. Its tick is a quarter of it, so an episode is
  # reported within 250 ms of the gap passing.
  @threshold_ms 1_000

  @wait_ms 60_000
  @poll_ms 200

  test "an entry channel's silence is one episode, and a frame re-arms the next" do
    namespace = unique("entry_idle")
    operative_id = unique("idle_probe")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port, _runner_id} = boot_runner(namespace, id_path)

    builder = %Builder{
      id: operative_id,
      factory_id: "entry_idle_probe",
      init_values: %{},
      opts: [plugins: [WorkflowResumeHarness.forward_sink("entry_idle_report", self())]]
    }

    assert {:ok, _info} = OperativeManager.start_operative(namespace, builder)

    # The reporting channel answers before anything has been fed, and the
    # accessor read absent at boot: a cold start is not an episode.
    assert cold = poll_report(operative_id, namespace, &newest/1),
           "the report channel must answer before the watched channel is fed"

    assert {true, 0, 0} = cold

    # One frame on the watched channel arms the latch. Nothing counts the
    # delivery: a cold start never fires, so the episode below is itself proof
    # that this frame landed.
    feed(operative_id, namespace, "a")

    # Then silence. Polling the report channel does not stamp the watched one,
    # so the gap keeps growing while this waits on it — and reaching this
    # assertion takes longer than the threshold, so several ticks have run.
    # One of them reported; the rest found the latch already fired.
    assert episode =
             poll_report(operative_id, namespace, fn {_, ep, _} = r -> ep == 1 && r end),
           "the silence must be reported as an idle episode after #{@threshold_ms} ms"

    assert {true, 1, 0} = episode

    # A frame ends the episode. `resumed` fires on the tick, up to a tick after
    # the frame itself, so the resuming activation's own report may still read
    # zero — this waits for the counter rather than asserting on that output.
    # The channel is silent again from here, so the next episode is due one
    # threshold later; this assertion lives inside that window.
    feed(operative_id, namespace, "b")

    assert resume =
             poll_report(operative_id, namespace, fn {_, _, res} = r -> res == 1 && r end),
           "the resuming frame must end the episode and re-arm the latch"

    assert {true, 1, 1} = resume
  end

  # --- helpers ----------------------------------------------------------------

  defp feed(operative_id, namespace, payload) do
    OperativeManager.send_io(operative_id, namespace, payload, %{"channel" => "idle_input"})
  end

  # Ask the report channel, then take the NEWEST report `pick` accepts among
  # everything received so far. Newest first, because the counters rise and an
  # older report that also satisfies `pick` is the wrong answer. The mailbox is
  # scanned, never consumed, so a later poll still sees everything.
  defp poll_report(operative_id, namespace, pick) do
    wait_until(
      fn ->
        OperativeManager.send_io(operative_id, namespace, "?", %{"channel" => "idle_report"})
        Enum.find_value(reports(), pick)
      end,
      @wait_ms,
      @poll_ms
    )
  end

  defp reports do
    {:messages, messages} = Process.info(self(), :messages)

    messages
    |> Enum.flat_map(fn
      {:message_forward, message, _metadata} -> [to_string(message)]
      _ -> []
    end)
    |> Enum.flat_map(&parse/1)
    |> Enum.reverse()
  end

  # The newest report, whatever it says.
  defp newest(report), do: report

  defp parse(line) do
    case String.split(line, ":") do
      [absent, episodes, resumes] ->
        [{absent == "true", to_int(episodes), to_int(resumes)}]

      _ ->
        []
    end
  end

  defp to_int(text) do
    case Integer.parse(text) do
      {value, ""} -> value
      _ -> -1
    end
  end
end
