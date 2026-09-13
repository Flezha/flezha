defmodule Flezha.Plugin.Provider.FileSinkTest do
  @moduledoc """
  The file sink: one line per record, synced before the outbox hold is
  answered, cut back and retried when a write fails, and a file it creates made
  private.

  The test process stands in for the writer that owns a guaranteed record, so a
  confirmation is an `{:outbox_confirmed_many, keys}` cast it receives.
  """

  # async: false: writes Application env; repoints the dead-letter store.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env, :isolated_dlq_dir], async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.File.Sink
  alias Flezha.Plugin.Provider.File.WriteWorker
  alias Flezha.State.TestHelpers
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
      assert [%{"error" => error}] = DlqStore.list("test-operative")
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
      :sys.replace_state(sink, &%{&1 | undo: {&1.inode, 0}})

      send(sink, :retry)
      assert_receive {:"$gen_cast", {:outbox_confirmed_many, keys}}, 2_000
      assert Enum.sort(keys) == [{:cursor, 1}, {:cursor, 2}, {:cursor, 3}]
      assert File.read(file) == {:ok, "r1\nr2\nr3\n"}
    end
  end

  describe "the commit option" do
    test "the default layout is append" do
      assert {:ok, opts} = prepare_sink(%{})
      assert opts.commit == "append"
    end

    test "a layout the option does not declare is refused naming the option" do
      assert {:error, message} = prepare_sink(%{commit: "rename"})
      assert message =~ "commit"
    end
  end

  describe "attach-time refusals" do
    test "the entry-side keys are refused as options this provider does not offer" do
      for key <- [:channel, :channel_in] do
        assert {:error, message} = prepare_sink(%{key => "events"})
        assert message =~ "unknown option(s) for file_sink: #{key}"
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

      {:ok, sink} = ProviderPrepare.activate(Sink, prepared)
      ProviderPrepare.stop_on_exit(Sink, sink)

      assert wait_until(
               fn -> health(prepared, sink) == {:unhealthy, {:open_failed, {:symlink, file}}} end,
               2_000
             )

      # What `flz operative info` renders beneath the plugin.
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
    {:ok, sink} = ProviderPrepare.activate(Sink, prepared)
    ProviderPrepare.stop_on_exit(Sink, sink)
    {prepared, sink}
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
    ctx = %Ctx.IO{operative_id: "op", namespace: "ns", message: message, metadata: metadata}
    assert {:ok, ^ctx} = Sink.on_output(ctx, opts)
  end

  # The test process stands in for the writer owning the record's entry.
  defp owner do
    entity_id = {"file_sink_#{System.unique_integer([:positive])}", "proc", "k1"}
    :ok = TestHelpers.stub_outbox_owner(entity_id)
    entity_id
  end

  # The worker opens the file before any record arrives.
  defp opened(file), do: assert(wait_until(fn -> File.exists?(file) end, 2_000))

  # The next write fails the way a real one does: the worker is handed a
  # descriptor to the same file that cannot be written through, and gives it up
  # after the failure, so the retry opens the file afresh.
  defp fail_next_write(sink, file) do
    opened(file)
    {:ok, read_only} = File.open(file, [:read])
    :sys.replace_state(sink, &%{&1 | fd: read_only})
  end

  defp health(opts, sink), do: Sink.health(Map.put(opts, :owned_pid, sink))
end
