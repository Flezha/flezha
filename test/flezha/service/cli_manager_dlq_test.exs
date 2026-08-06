defmodule Flezha.Service.CliManagerDlqTest do
  @moduledoc """
  `flz dlq list | show | replay | drop` controller-side behaviour: replay
  re-enters the ordinary `:io_in` input path, a clean replay removes the
  entry, a failing replay re-parks it with `generation` incremented, and
  a drop is audited.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.Dlq.Store
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Service.CliManager

  setup do
    dir = Path.join(System.tmp_dir!(), "flezha_dlq_cli_#{System.unique_integer([:positive])}")
    original = Application.get_env(:flezha, :dlq, [])

    Application.put_env(
      :flezha,
      :dlq,
      Keyword.merge(original, dir: dir, replay_grace_ms: 400)
    )

    ClassRegistry.ensure_table()

    on_exit(fn ->
      Application.put_env(:flezha, :dlq, original)
      File.rm_rf!(dir)
    end)

    rc = "dlqcli_" <> Integer.to_string(System.unique_integer([:positive]))

    entry = %{
      operative_class: rc,
      procedure_id: "proc",
      entity_key: "k",
      channel: "input",
      origin_offset: nil,
      error: "kaboom",
      attempts: 3,
      payload: "poison-bytes"
    }

    %{rc: rc, entry: entry}
  end

  test "list and show render the parked entry", %{entry: entry} do
    {:ok, ref} = Store.append(entry)

    listing = capture_io(fn -> CliManager.dlq_list(entry.operative_class) end)
    assert listing =~ ref
    assert listing =~ "input"
    assert listing =~ "kaboom"

    shown = capture_io(fn -> CliManager.dlq_show(ref) end)
    assert shown =~ ~s("kaboom")
    assert shown =~ "poison-bytes"
    assert shown =~ ~s("parked")
  end

  test "replay re-enters the normal input path; success removes the entry", %{
    rc: rc,
    entry: entry
  } do
    {:ok, ref} = Store.append(entry)
    ClassRegistry.put_overwatch(rc, self())

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)

    assert_received {:"$gen_cast", {:io_in, "poison-bytes", %{"channel" => "input"}}}
    assert output =~ "replay succeeded"
    assert {:error, :not_found} = Store.get(ref)
  end

  test "failing replay re-parks with generation 2", %{rc: rc, entry: entry} do
    {:ok, ref} = Store.append(entry)

    # Stand-in for the runner's retry-then-park cycle: on receiving the
    # replayed input, park the same payload again (same content identity).
    parent = self()

    overwatch =
      spawn_link(fn ->
        receive do
          {:"$gen_cast", {:io_in, payload, _metadata}} ->
            {:ok, _} = Store.append(%{entry | payload: payload, attempts: 3})
            send(parent, :re_parked)
        end
      end)

    ClassRegistry.put_overwatch(rc, overwatch)

    output = capture_io(fn -> CliManager.dlq_replay(ref) end)

    assert_received :re_parked
    assert output =~ "re-parked with generation 2"
    assert {:ok, still_there, _payload} = Store.get(ref)
    assert still_there["generation"] == 2
    assert still_there["status"] == "parked"
  end

  test "drop removes the entry and emits the audit event", %{entry: entry} do
    {:ok, ref} = Store.append(entry)

    handler = :"dlq_cli_test_#{System.unique_integer([:positive])}"
    parent = self()

    :telemetry.attach(
      handler,
      [:flezha, :dlq, :operator_drop],
      fn _e, m, md, _c -> send(parent, {:audit, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    output = capture_io(fn -> CliManager.dlq_drop(ref) end)

    assert output =~ "dropped #{ref}"
    assert_received {:audit, %{count: 1}, %{entry: ^ref, caller_source: :cli}}
    assert {:error, :not_found} = Store.get(ref)
  end
end
