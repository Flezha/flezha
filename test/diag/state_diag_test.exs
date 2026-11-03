defmodule FlezhaTest.Diag.StateDiagTest do
  # Temporary: prints what a reopened writer reads, for the CI-only failure of
  # the park re-drive test. Removed once the cause is known.
  use FlezhaTest.OutboxCase, async: false

  defp show(label, cell_id) do
    dir = cell_id |> Paths.outbox_dedup_path() |> Path.dirname()

    files =
      for f <- Path.wildcard(Path.join(dir, "**"), match_dot: true), File.regular?(f) do
        {Path.relative_to(f, dir), File.stat!(f).size}
      end

    IO.puts(
      "DIAG #{label}\n  read=#{inspect(Lineage.read(cell_id))}\n  dir=#{dir}\n  files=#{inspect(files)}"
    )
  end

  defp collect_for(deadline, acc) do
    left = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      m -> collect_for(deadline, [m | acc])
    after
      left -> Enum.reverse(acc)
    end
  end

  test "park re-drive, step by step", %{rc: rc, cell_id: cell_id, base: base} do
    Logger.configure(level: :debug)
    on_exit(fn -> Logger.configure(level: :warning) end)
    IO.puts("DIAG schedulers=#{System.schedulers_online()} tmp=#{System.tmp_dir!()} base=#{base}")

    redirect_dlq_dir(base)
    pid = armed_writer(rc, cell_id)

    :ok =
      WalWriter.append(
        pid,
        0,
        effect_header(),
        effect_body([], 5, %{channel: "in", error: "kaboom", attempts: 3, payload: "poison"})
      )

    append_effect(pid, 1, [output("k", "marker")], 6)
    IO.puts("DIAG first output=#{inspect(next_output())}")
    show("before close", cell_id)
    close_writer(pid, cell_id)
    show("after close", cell_id)

    dedup = Lineage.progress(cell_id)
    :ok = Lineage.put_dedup(cell_id, %{dedup | outbox_flushed_seq: -1})
    show("after stale cursor", cell_id)

    pid = start_writer(cell_id)
    IO.puts("DIAG last_persisted=#{inspect(WalWriter.last_persisted_seq(pid))}")
    show("after reopen", cell_id)
    IO.puts("DIAG writer state=#{inspect(:sys.get_state(pid), limit: 60, printable_limit: 200)}")

    msgs = collect_for(System.monotonic_time(:millisecond) + 3_000, [])

    IO.puts("DIAG messages after reopen=#{inspect(msgs, limit: 30)}")
    IO.puts("DIAG overwatch=#{inspect(ClassRegistry.overwatch_pid(rc))}")
  end
end
