defmodule Flezha.Plugin.Provider.FileSinkTest do
  @moduledoc """
  The file sink: one line per record, into one growing file under `append` and
  into parts committed by rename under `rename`, durable before the outbox hold
  is answered, cut back and retried when a write fails, and a file it creates
  made private.

  The test process stands in for the writer that owns a guaranteed record, so a
  confirmation is an `{:outbox_confirmed_many, keys}` cast it receives.
  """

  # async: false: writes Application env; repoints the dead-letter store.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env, :system_env, :isolated_dlq_dir], async: false

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 2]

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Http.S3
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.File.Sink
  alias Flezha.Plugin.Provider.File.WriteWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.Minio
  alias FlezhaTest.Helper.ProviderPrepare

  # A retry parked at a minute keeps a failed batch failed until the test says so.
  @held_retry %{retry_backoff_ms: 60_000, retry_max_backoff_ms: 60_000}

  setup %{tmp: root} do
    Application.put_env(:flezha, :file_plugin_root, root)
    %{root: root, out: Path.join(root, "out.ndjson")}
  end

  describe "one line per record" do
    test "three records become three lines", %{out: file} do
      {opts, _sink} = start_sink()
      for record <- ["a", "b", "c"], do: release(opts, record)

      assert wait_until(fn -> File.read(file) == {:ok, "a\nb\nc\n"} end, 2_000)
    end

    test "only the filtered channel is written", %{out: file} do
      {opts, _sink} = start_sink(%{channel_out: "alerts"})
      release(opts, "metric", %{"channel" => "metrics"})
      release(opts, "alert", %{"channel" => "alerts"})

      assert wait_until(fn -> File.read(file) == {:ok, "alert\n"} end, 2_000)
    end

    test "a record holding a newline is parked, not written", %{out: file} do
      owner = owner()
      {opts, _sink} = start_sink()
      release(opts, "a\nb", TestHelpers.guaranteed_metadata(owner, 1))

      # Its hold is answered once the park is durable.
      assert_receive {:"$gen_cast", {:outbox_confirmed, {:cursor, 1}}}, 5_000
      assert [%{"error" => error}] = DlqStore.list("test-operator")
      assert error =~ "embedded_newline"
      assert File.read(file) == {:ok, ""}
    end
  end

  describe "durability" do
    test "a hold is confirmed only once the batch holding it is synced", %{out: file} do
      owner = owner()
      {opts, sink} = start_sink(@held_retry)
      fail_next_write(sink, file)

      release(opts, "a", TestHelpers.guaranteed_metadata(owner, 1))
      assert wait_until(fn -> WriteWorker.stats(sink).consecutive_failures == 1 end, 2_000)
      refute_received {:"$gen_cast", {:outbox_confirmed_many, _}}
      assert File.read(file) == {:ok, ""}

      send(sink, :retry)
      assert_receive {:"$gen_cast", {:outbox_confirmed_many, [{:cursor, 1}]}}, 2_000
      assert File.read(file) == {:ok, "a\n"}
    end

    test "a failed write drops nothing, and a half-written batch is cut back first",
         %{out: file} do
      owner = owner()
      {opts, sink} = start_sink(Map.merge(@held_retry, %{linger_ms: 60_000, max_batch: 3}))
      fail_next_write(sink, file)

      for seq <- 1..3, do: release(opts, "r#{seq}", TestHelpers.guaranteed_metadata(owner, seq))
      assert wait_until(fn -> WriteWorker.stats(sink).consecutive_failures == 1 end, 2_000)

      # What a write that failed part-way leaves behind, with a descriptor too
      # broken to cut it: the cut is owed to the next open of the same file.
      File.write!(file, "r1\nr", [:append])
      :sys.replace_state(sink, &%{&1 | io: %{&1.io | undo: {&1.io.inode, 0}}})

      send(sink, :retry)
      assert_receive {:"$gen_cast", {:outbox_confirmed_many, keys}}, 2_000
      assert Enum.sort(keys) == [{:cursor, 1}, {:cursor, 2}, {:cursor, 3}]
      assert File.read(file) == {:ok, "r1\nr2\nr3\n"}
    end

    test "append writes a batch again when its confirmation was lost in a crash", %{out: file} do
      {cell_id, records} = unconfirmed_batch()
      {opts, sink} = start_sink()
      hand_over(opts, records)
      assert wait_until(fn -> File.read(file) == {:ok, "a\nb\n"} end, 2_000)

      crash(opts, sink)
      activate(opts)
      :ok = TestHelpers.stub_outbox_owner(cell_id)
      hand_over(opts, records)

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert File.read(file) == {:ok, "a\nb\na\nb\n"}
    end
  end

  describe "the commit option" do
    test "the default layout is append" do
      assert {:ok, opts} = prepare_sink(%{})
      assert opts.commit == "append"
    end

    test "a layout the option does not declare is refused naming the option" do
      assert {:error, {:invalid_option_value, :commit, "truncate"}} =
               prepare_sink(%{commit: "truncate"})
    end
  end

  describe "commit: rename" do
    test "rename makes the directory private, and takes one that exists", %{root: root} do
      assert {:ok, opts} = prepare_sink(rename(%{}))
      assert File.dir?(opts.path)
      assert Bitwise.band(File.stat!(opts.path).mode, 0o777) == 0o700

      File.mkdir!(Path.join(root, "existing"))
      assert {:ok, _opts} = prepare_sink(rename(%{path: "existing"}))
    end

    test "a path that is a regular file is refused naming the layout", %{out: file} do
      File.write!(file, "")

      assert {:error, {:file_path_not_directory, _dir}} =
               prepare_sink(rename(%{path: "out.ndjson"}))
    end

    test "a roll becomes one immutable part", %{root: root} do
      dir = Path.join(root, "out")
      {opts, _sink} = start_sink(rename(%{roll_after_ms: 100}))
      for record <- ["a", "b", "c"], do: release(opts, record)

      assert wait_until(fn -> committed(dir) == "a\nb\nc\n" end, 2_000)
      assert Enum.sort(File.ls!(dir)) == [".progress", "part-00000001"]
      assert progress(dir)["part"] == 1

      for name <- ["part-00000001", ".progress"],
          do: assert(Bitwise.band(File.stat!(Path.join(dir, name)).mode, 0o777) == 0o600, name)
    end

    test "a hold is confirmed when its part is committed, not when its batch syncs",
         %{root: root} do
      dir = Path.join(root, "out")
      owner = owner()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 60_000}))
      release(opts, "a", TestHelpers.guaranteed_metadata(owner, 1))

      assert wait_until(fn -> open_roll(dir) == "a\n" end, 2_000)
      refute_received {:"$gen_cast", {:outbox_confirmed_many, _}}
      refute File.exists?(Path.join(dir, ".progress"))

      send(sink, :roll)
      assert_confirmed([{:cursor, 1}])
      assert progress(dir)["part"] == 1
    end

    test "a crash before the part rename writes the records once", %{root: root} do
      dir = Path.join(root, "out")
      {cell_id, records} = unconfirmed_batch()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 60_000}))
      hand_over(opts, records)
      assert wait_until(fn -> open_roll(dir) == "a\nb\n" end, 2_000)

      crash(opts, sink)
      activate(%{opts | roll_after_ms: 1})
      :ok = TestHelpers.stub_outbox_owner(cell_id)
      hand_over(opts, records)

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert committed(dir) == "a\nb\n"
      assert Enum.sort(File.ls!(dir)) == [".progress", "part-00000001"]
    end

    test "a crash between the part rename and the sidecar writes the records once",
         %{root: root} do
      dir = Path.join(root, "out")
      {cell_id, [first, second]} = unconfirmed_batch()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 1}))
      hand_over(opts, [first])
      assert wait_until(fn -> committed(dir) == "a\n" end, 2_000)
      progress_one = File.read!(Path.join(dir, ".progress"))
      hand_over(opts, [second])
      assert wait_until(fn -> committed(dir) == "a\nb\n" end, 2_000)

      # What a crash after `part-00000002` was renamed and before its sidecar leaves.
      crash(opts, sink)
      File.write!(Path.join(dir, ".progress"), progress_one)
      activate(opts)
      assert wait_until(fn -> not File.exists?(Path.join(dir, "part-00000002")) end, 2_000)

      :ok = TestHelpers.stub_outbox_owner(cell_id)
      hand_over(opts, [first, second])

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert committed(dir) == "a\nb\n"
      assert File.read!(Path.join(dir, "part-00000002")) == "b\n"
    end

    test "a crash after the sidecar, before the confirmation, writes the records once",
         %{root: root} do
      dir = Path.join(root, "out")
      {cell_id, records} = unconfirmed_batch()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 1}))
      hand_over(opts, records)
      assert wait_until(fn -> committed(dir) == "a\nb\n" end, 2_000)

      crash(opts, sink)
      activate(opts)
      :ok = TestHelpers.stub_outbox_owner(cell_id)
      hand_over(opts, records)

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert committed(dir) == "a\nb\n"
      assert temp_files(dir) == []
    end

    test "a part more than one ahead of the sidecar refuses the start and deletes nothing",
         %{root: root} do
      dir = Path.join(root, "out")
      {opts, sink} = start_sink(rename(%{roll_after_ms: 1}))
      release(opts, "a")
      assert wait_until(fn -> committed(dir) == "a\n" end, 2_000)
      progress_one = File.read!(Path.join(dir, ".progress"))

      for {record, written} <- [{"b", "a\nb\n"}, {"c", "a\nb\nc\n"}] do
        release(opts, record)
        assert wait_until(fn -> committed(dir) == written end, 2_000)
      end

      crash(opts, sink)
      File.write!(Path.join(dir, ".progress"), progress_one)
      sink = activate(opts)

      refused = {:unhealthy, {:open_failed, {{:part_ahead_of_progress, 3, 1}, opts.path}}}
      assert wait_until(fn -> health(opts, sink) == refused end, 2_000)

      assert Enum.sort(File.ls!(dir)) ==
               [".progress", "part-00000001", "part-00000002", "part-00000003"]
    end

    test "a record the outbox does not guarantee is written, and no watermark names it",
         %{root: root} do
      dir = Path.join(root, "out")
      {opts, _sink} = start_sink(rename(%{roll_after_ms: 1}))
      # Keyed off the tier: a Cell and a seq but no confirmation token, so one
      # seq may name two records.
      keyed = %{"channel" => "out", "flz.cell" => "op/task/k1", "flz.seq" => "7.0"}
      release(opts, "plain")
      release(opts, "first", keyed)
      release(opts, "second", keyed)

      assert wait_until(fn -> committed(dir) == "plain\nfirst\nsecond\n" end, 2_000)
      assert progress(dir)["watermarks"] == %{}
    end

    test "a roll that reaches roll_bytes is committed before roll_after_ms", %{root: root} do
      dir = Path.join(root, "out")
      {opts, _sink} = start_sink(rename(%{roll_bytes: 64, roll_after_ms: 60_000}))
      for _record <- 1..10, do: release(opts, String.duplicate("x", 9))

      assert wait_until(fn -> File.exists?(Path.join(dir, "part-00000001")) end, 2_000)
    end

    test "a roll whose commit keeps failing is unhealthy with nothing queued behind it",
         %{root: root} do
      dir = Path.join(root, "out")
      owner = owner()
      extra = Map.merge(@held_retry, %{roll_after_ms: 60_000, unhealthy_after_ms: 1})
      {opts, sink} = start_sink(rename(extra))
      release(opts, "a", TestHelpers.guaranteed_metadata(owner, 1))
      assert wait_until(fn -> open_roll(dir) == "a\n" end, 2_000)

      # A directory the worker cannot rename in: the part rename fails.
      File.chmod!(dir, 0o500)
      on_exit(fn -> File.chmod(dir, 0o700) end)
      send(sink, :roll)

      assert wait_until(
               fn -> match?({:unhealthy, {:write_stalled, :eacces}}, health(opts, sink)) end,
               2_000
             )

      File.chmod!(dir, 0o700)
      send(sink, :retry)
      assert_confirmed([{:cursor, 1}])
      assert committed(dir) == "a\n"
    end
  end

  describe "under an S3 root" do
    setup do
      Minio.file_plugin_root("s3://events/flezha")
    end

    test "append, given or taken as the default, is refused naming commit" do
      for extra <- [%{path: "out"}, %{path: "out", commit: "append"}] do
        assert {:error, {:file_commit_unsupported, "s3://events/flezha/out", "append"}} =
                 prepare_sink(extra)
      end
    end

    test "a roll above 5 GiB is refused naming roll_bytes" do
      assert {:error, {:option_above_maximum, :roll_bytes, 6_442_450_944, _max}} =
               prepare_sink(rename(%{roll_bytes: 6 * 1024 ** 3}))

      assert {:ok, %{path: "s3://events/flezha/out"}} =
               prepare_sink(rename(%{roll_bytes: 5 * 1024 ** 3}))
    end
  end

  # Against `FlezhaTest.Helper.FakeS3`, or MinIO under `S3_INTEGRATION=1`.
  describe "commit: rename under an S3 root" do
    if Minio.integration?(), do: @describetag(:s3_integration)

    setup do
      Minio.plugin_bucket!("flezha-file-sink-test")
    end

    test "a roll becomes one object, and its holds wait for the sidecar", %{s3: s3} do
      owner = owner()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 60_000}))
      for seq <- 1..2, do: release(opts, "r#{seq}", TestHelpers.guaranteed_metadata(owner, seq))

      assert wait_until(fn -> :sys.get_state(sink).roll.bytes == 6 end, 2_000)
      assert keys(s3, "out") == []
      refute_received {:"$gen_cast", {:outbox_confirmed_many, _}}

      send(sink, :roll)

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert keys(s3, "out") == [".progress", "part-00000001"]
      assert S3.get("out/part-00000001", s3) == {:ok, "r1\nr2\n"}
      assert progress(bucket(s3, "out"))["part"] == 1
    end

    test "a crash between the part and the sidecar writes the records once", %{s3: s3} do
      {cell_id, records} = unconfirmed_batch()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 60_000}))
      hand_over(opts, records)
      assert wait_until(fn -> :sys.get_state(sink).roll.bytes == 4 end, 2_000)

      Minio.hold_guarded_writes!(sink)
      send(sink, :roll)
      assert_receive {:held, ^sink}, 5_000
      assert keys(s3, "out") == ["part-00000001"]

      crash(opts, sink)
      activate(%{opts | roll_after_ms: 1})
      assert wait_until(fn -> keys(s3, "out") == [] end, 5_000)

      :ok = TestHelpers.stub_outbox_owner(cell_id)
      hand_over(opts, records)

      assert_confirmed([{:cursor, 1}, {:cursor, 2}])
      assert committed(bucket(s3, "out")) == "a\nb\n"
      assert keys(s3, "out") == [".progress", "part-00000001"]
    end

    test "a part more than one ahead of the sidecar refuses the start and deletes nothing",
         %{s3: s3} do
      :ok = S3.put_if("out/.progress", ~s({"part":3,"watermarks":{}}), 3, &(&1 <= 3), s3)
      :ok = S3.put("out/part-00000005", "e\n", [], s3)
      {opts, sink} = start_sink(rename(%{}))

      refused = {:unhealthy, {:open_failed, {{:part_ahead_of_progress, 5, 3}, opts.path}}}
      assert wait_until(fn -> health(opts, sink) == refused end, 5_000)
      assert keys(s3, "out") == [".progress", "part-00000005"]
    end

    test "a worker behind a sidecar another worker advanced stops without confirming",
         %{s3: s3} do
      owner = owner()
      {opts, sink} = start_sink(rename(%{roll_after_ms: 60_000}))
      release(opts, "a", TestHelpers.guaranteed_metadata(owner, 1))
      assert wait_until(fn -> :sys.get_state(sink).roll.bytes == 2 end, 2_000)
      :ok = S3.put_if("out/.progress", ~s({"part":7,"watermarks":{}}), 7, &(&1 <= 7), s3)

      ref = Process.monitor(sink)
      on_exit(override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          send(sink, :roll)
          assert_receive {:DOWN, ^ref, :process, ^sink, {:progress_ahead, 7}}, 5_000
          assert wait_until(fn -> WorkerSup.worker_pid(Sink, opts.plugin_id) == nil end, 2_000)
        end)

      assert log =~ "progress names part 7"
      refute_received {:"$gen_cast", {:outbox_confirmed_many, _}}
      assert progress(bucket(s3, "out"))["part"] == 7
    end
  end

  describe "attach-time refusals" do
    test "the entry-side keys are refused as options this provider does not offer" do
      for key <- [:channel, :channel_in] do
        name = Atom.to_string(key)

        assert {:error, {:unknown_options, "file_sink", [^name]}} =
                 prepare_sink(%{key => "events"})
      end
    end
  end

  describe "the file it opens" do
    test "a file the sink creates is private", %{out: file} do
      start_sink()
      opened(file)

      assert Bitwise.band(File.stat!(file).mode, 0o777) == 0o600
    end

    test "a file that already exists keeps its mode", %{out: file} do
      File.write!(file, "")
      File.chmod!(file, 0o644)
      {opts, _sink} = start_sink()
      release(opts, "a")

      assert wait_until(fn -> File.read(file) == {:ok, "a\n"} end, 2_000)
      assert Bitwise.band(File.stat!(file).mode, 0o777) == 0o644
    end

    # The root check follows a link that is already there when the plugin is
    # enabled; the open refuses one the path has become since.
    test "a path that is a symlink when it is opened is not opened", %{root: root, out: file} do
      target = Path.join(root, "target.ndjson")
      File.write!(target, "")
      assert {:ok, prepared} = prepare_sink(%{})
      File.ln_s!(target, file)

      sink = activate(prepared)

      assert wait_until(
               fn -> health(prepared, sink) == {:unhealthy, {:open_failed, {:symlink, file}}} end,
               2_000
             )

      # What `flz operator info` renders beneath the plugin.
      assert Sink.inspect_state(Map.put(prepared, :owned_pid, sink)) ==
               %{path: file, health: {:unhealthy, {:open_failed, {:symlink, file}}}}

      assert File.read(target) == {:ok, ""}
    end
  end

  describe "health" do
    test "a stalled flush is unhealthy", %{out: file} do
      {opts, sink} = start_sink(Map.put(@held_retry, :unhealthy_after_ms, 1))
      fail_next_write(sink, file)
      release(opts, "a")

      assert wait_until(
               fn -> match?({:unhealthy, {:write_stalled, _error}}, health(opts, sink)) end,
               2_000
             )
    end

    test "an idle sink is healthy", %{out: file} do
      {opts, sink} = start_sink()
      opened(file)

      assert health(opts, sink) == :healthy
    end
  end

  defp start_sink(extra \\ %{}) do
    {:ok, prepared} = prepare_sink(extra)
    {prepared, activate(prepared)}
  end

  # What activation starts, and the stop that ends it with the test; also the
  # restart after a crash.
  defp activate(prepared) do
    {:ok, sink} = ProviderPrepare.activate(Sink, prepared)
    ProviderPrepare.stop_on_exit(Sink, sink)
    sink
  end

  defp prepare_sink(extra) do
    ProviderPrepare.prepare(
      Sink,
      Map.merge(
        %{
          path: "out.ndjson",
          linger_ms: 1,
          plugin_id: "file-sink-#{System.unique_integer([:positive])}",
          subscribed_extensions: [:on_output]
        },
        extra
      )
    )
  end

  defp release(opts, message, metadata \\ %{"channel" => "out"}) do
    ctx = %Ctx.IO{operator_id: "op", namespace: "ns", message: message, metadata: metadata}
    assert {:ok, ^ctx} = Sink.on_output(ctx, opts)
  end

  # The test process stands in for the writer owning the record's entry.
  defp owner do
    cell_id = cell_id()
    :ok = TestHelpers.stub_outbox_owner(cell_id)
    cell_id
  end

  defp cell_id, do: {"file_sink_#{System.unique_integer([:positive])}", "task", "k1"}

  # Two guaranteed records whose owner is not registered yet, so the first
  # confirmation reaches nobody: what the outbox sees of a controller that died
  # between the sync and the report. Handing them over again is the re-drive.
  defp unconfirmed_batch do
    cell_id = cell_id()

    {cell_id,
     [
       {"a", TestHelpers.guaranteed_metadata(cell_id, 1)},
       {"b", TestHelpers.guaranteed_metadata(cell_id, 2)}
     ]}
  end

  defp hand_over(opts, records),
    do: for({msg, metadata} <- records, do: release(opts, msg, metadata))

  defp rename(extra), do: Map.merge(%{path: "out", commit: "rename"}, extra)

  # A kill, so the worker runs no `terminate/2`. Its supervisor reports the
  # killed child, so the wait for the supervisor to drop it runs inside the
  # capture.
  defp crash(opts, sink) do
    supervisor = WorkerSup.dyn_sup_name(WorkerSup.family(Sink))

    capture_log(fn ->
      Process.exit(sink, :kill)

      assert wait_until(
               fn ->
                 WorkerSup.worker_pid(Sink, opts.plugin_id) == nil and
                   not Enum.any?(
                     DynamicSupervisor.which_children(supervisor),
                     &match?({_, ^sink, _, _}, &1)
                   )
               end,
               2_000
             )
    end)
  end

  # A batch may be split across flushes, so the confirmations are gathered
  # until as many keys have arrived as are expected.
  defp assert_confirmed(keys, seen \\ []) do
    if length(seen) >= length(keys) do
      assert Enum.sort(seen) == Enum.sort(keys)
    else
      assert_receive {:"$gen_cast", {:outbox_confirmed_many, more}}, 2_000
      assert_confirmed(keys, seen ++ more)
    end
  end

  # What a consumer that reads only parts at or below the sidecar's number sees,
  # of a folder or of the objects under a key prefix.
  defp committed(dir) when is_binary(dir), do: committed(folder(dir))

  defp committed({names, read} = listing) do
    case progress(listing) do
      %{"part" => last} ->
        names
        |> Enum.filter(
          &(&1 =~ ~r/^part-\d+$/ and String.to_integer(String.trim_leading(&1, "part-")) <= last)
        )
        |> Enum.sort()
        |> Enum.map_join(read)

      _no_part_yet ->
        nil
    end
  end

  defp progress(dir) when is_binary(dir), do: progress(folder(dir))

  defp progress({names, read}),
    do: if(".progress" in names, do: Jason.decode!(read.(".progress")), else: %{})

  # A directory as its names and a reader of one name.
  defp folder(dir), do: {File.ls!(dir), &File.read!(Path.join(dir, &1))}

  defp bucket(s3, dir) do
    {:ok, entries} = S3.list(dir <> "/", s3)
    names = Enum.map(entries, fn {key, _age} -> String.replace_prefix(key, dir <> "/", "") end)

    {names,
     fn name ->
       {:ok, body} = S3.get(dir <> "/" <> name, s3)
       body
     end}
  end

  defp keys(s3, dir), do: s3 |> bucket(dir) |> elem(0) |> Enum.sort()

  defp temp_files(dir), do: dir |> File.ls!() |> Enum.filter(&String.ends_with?(&1, ".tmp"))

  # The open roll's bytes: what a crash before the part rename leaves behind.
  defp open_roll(dir) do
    case temp_files(dir) do
      [temp] -> File.read!(Path.join(dir, temp))
      _none_or_several -> nil
    end
  end

  # The worker opens the file before any record arrives.
  defp opened(file), do: assert(wait_until(fn -> File.exists?(file) end, 2_000))

  # The next write fails the way a real one does: the worker is handed a
  # descriptor to the same file that cannot be written through, and gives it up
  # after the failure, so the retry opens the file afresh.
  defp fail_next_write(sink, file) do
    opened(file)
    {:ok, read_only} = File.open(file, [:read])
    :sys.replace_state(sink, &%{&1 | io: %{&1.io | fd: read_only}})
  end

  defp health(opts, sink), do: Sink.health(Map.put(opts, :owned_pid, sink))
end
