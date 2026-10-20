defmodule Flezha.Plugin.Provider.FileSourceTest do
  @moduledoc """
  The file source: one line of a file is one entry frame, injected through the
  confirming ingest, followed as the file grows, and resumed from a committed
  offset.

  The test process stands in for the host relay, so every injection is an
  `{:ingest, line, metadata}` call the test answers and every commit is a
  `{:plugin_opt_persist, …}` cast it receives. Polls after the first are driven
  by sending `:poll`, with `poll_interval_ms` parked at a minute, unless the
  scenario is about the interval itself.
  """

  # async: false: writes Application env.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env, :system_env], async: false

  import ExUnit.CaptureIO
  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 2]

  import FlezhaTest.Helper.ProviderPrepare,
    only: [take_ingest: 0, take_ingest: 1, refute_ingest: 0]

  alias Flezha.Http.S3
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.File.Source
  alias Flezha.Plugin.Providers
  alias Flezha.Service.Cli
  alias FlezhaTest.Helper.Minio
  alias FlezhaTest.Helper.ProviderPrepare

  @channel "events"

  @catalog Host.build_catalog(
             [{:on_terminate, :fold, Ctx.Terminate}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  setup %{tmp: root} do
    Application.put_env(:flezha, :file_plugin_root, root)
    %{events: Path.join(root, "events.ndjson")}
  end

  describe "the provider id" do
    test "an enable naming relay_file_input prepares and activates on a relay's table",
         %{events: file} do
      File.write!(file, "")

      assert {:ok, provider} = Providers.fetch("relay_file_input"),
             "relay_file_input is not a provider id the system can enable"

      plugin = %Plugin{
        id: "file-source-enable",
        provider: provider,
        namespace: :default,
        relay_id: "file-source-op",
        opts: %{path: "events.ndjson", channel_in: @channel},
        hooks: [{:on_terminate, []}]
      }

      assert {:ok, table} = Table.bulk_subscribe(Table.new(@catalog), [{plugin.id, plugin}])
      assert {table, []} = Table.bulk_activate(table, %{})
      assert Table.running?(table, plugin.id)

      _ = Table.teardown_all(table)
    end
  end

  describe "one line, one frame" do
    test "three lines become three frames on the entry channel, in order", %{events: file} do
      File.write!(file, "a\nb\nc\n")
      start_reader()

      for line <- ["a", "b", "c"] do
        assert {^line, %{"channel" => @channel}} = take_ingest()
      end

      assert_committed(6)
      refute_ingest()
    end

    test "an empty line is delivered as an empty string", %{events: file} do
      File.write!(file, "a\n\nb\n")
      start_reader()

      for line <- ["a", "", "b"], do: assert({^line, _} = take_ingest())
    end

    test "a fragment waits for its newline", %{events: file} do
      File.write!(file, "a\nb")
      {_opts, reader} = start_reader()

      assert {"a", _} = take_ingest()
      assert_committed(2)

      send(reader, :poll)
      settle(reader)
      refute_ingest()

      File.write!(file, "\n", [:append])
      send(reader, :poll)
      assert {"b", _} = take_ingest()
      assert_committed(4)
    end

    test "a refused ingest is offered again and the reader does not pass it", %{events: file} do
      File.write!(file, "a\nb\n")
      {_opts, reader} = start_reader()

      assert {"a", _} = take_ingest({:error, :refused})
      assert_committed(0)
      refute_ingest()

      send(reader, :poll)
      assert {"a", _} = take_ingest()
      assert {"b", _} = take_ingest()
      assert_committed(4)
    end
  end

  describe "following the file" do
    test "an appended line is delivered within one poll interval", %{events: file} do
      File.write!(file, "a\n")
      start_reader(%{poll_interval_ms: 50})
      assert {"a", _} = take_ingest()

      File.write!(file, "d\n", [:append])
      assert {"d", _} = take_ingest()
    end

    test "start_at end ignores what the file already holds", %{events: file} do
      File.write!(file, "a\nb\nc\n")
      {_opts, reader} = start_reader(%{start_at: "end"})

      # The placement is committed, so a restart does not re-measure the end.
      assert_committed(6)
      refute_ingest()

      File.write!(file, "d\n", [:append])
      send(reader, :poll)
      assert {"d", _} = take_ingest()
    end

    test "a truncated file is read again from byte 0, and says so", %{events: file} do
      File.write!(file, String.duplicate("x", 299) <> "\n")
      {_opts, reader} = start_reader()
      assert {_line, _} = take_ingest()
      assert_committed(300)

      on_exit(override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          File.write!(file, "x\n")
          send(reader, :poll)
          assert {"x", _} = take_ingest()
          assert_committed(2)
        end)

      assert log =~ "truncated or replaced path=#{file} offset=300"
    end

    test "a replaced file is read from byte 0 even when it is longer", %{events: file} do
      File.write!(file, "a\nb\n")
      {_opts, reader} = start_reader()
      assert {"a", _} = take_ingest()
      assert {"b", _} = take_ingest()
      assert_committed(4)

      replacement = file <> ".next"
      File.write!(replacement, "c\nd\ne\n")
      File.rename!(replacement, file)

      send(reader, :poll)
      assert {"c", _} = take_ingest()
    end
  end

  describe "resuming" do
    test "a plugin restart resumes after the last committed line", %{events: file} do
      File.write!(file, "a\nb\nc\n")
      {opts, reader} = start_reader()
      for line <- ["a", "b", "c"], do: assert({^line, _} = take_ingest())
      assert_committed(6)

      :ok = ProviderPrepare.stop(Source, reader)
      File.write!(file, "d\n", [:append])

      # The table re-prepares from the options it holds, and the commit wrote the
      # offset onto them.
      start_reader(%{plugin_id: opts.plugin_id, offset: 6})
      assert {"d", _} = take_ingest()
      assert_committed(8)
      refute_ingest()
    end

    test "a crash before the commit re-delivers what followed it and skips nothing",
         %{events: file} do
      File.write!(file, "a\nb\nc\n")
      {opts, reader} = start_reader()
      for line <- ["a", "b", "c"], do: assert({^line, _} = take_ingest())
      assert_committed(6)

      File.write!(file, "d\ne\n", [:append])
      send(reader, :poll)
      assert {"d", _} = take_ingest()
      assert {"e", _} = take_ingest()

      # This commit is the one the crash loses: the next reader starts from 6.
      assert_committed(10)
      :ok = ProviderPrepare.stop(Source, reader)

      File.write!(file, "f\n", [:append])
      start_reader(%{plugin_id: opts.plugin_id, offset: 6})
      for line <- ["d", "e", "f"], do: assert({^line, _} = take_ingest())
    end
  end

  describe "bounds and health" do
    test "an oversized line stops the reader at its offset, naming the offset and not the bytes",
         %{events: file} do
      lines = for i <- 1..4, do: "line#{i}xxxx"
      secret = String.duplicate("s", 20)
      File.write!(file, Enum.map_join(lines, &(&1 <> "\n")) <> secret <> "\nafter\n")

      on_exit(override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          {opts, reader} = start_reader(%{max_line_bytes: 16})
          for line <- lines, do: assert({^line, _} = take_ingest())
          assert_committed(40)
          assert health(opts, reader) == {:unhealthy, {:line_too_long, 40}}

          send(reader, :poll)
          settle(reader)
          refute_ingest()
        end)

      assert log =~ "line_too_long path=#{file} offset=40"
      refute log =~ secret
    end

    test "a line that is not UTF-8 stops the reader at its offset", %{events: file} do
      File.write!(file, "ok\n" <> <<0xFF, 0xFE>> <> "\n")
      {opts, reader} = start_reader()

      assert {"ok", _} = take_ingest()
      assert_committed(3)
      assert health(opts, reader) == {:unhealthy, {:invalid_utf8, 3}}
    end

    test "a missing file is unhealthy, and recovers when it appears", %{events: file} do
      {opts, reader} = start_reader(%{poll_interval_ms: 50})

      assert wait_until(
               fn -> health(opts, reader) == {:unhealthy, {:file_missing, file}} end,
               2_000
             )

      # What `flz relay info` renders beneath the plugin.
      assert Source.inspect_state(Map.put(opts, :owned_pid, reader)) ==
               %{path: file, health: {:unhealthy, {:file_missing, file}}}

      File.write!(file, "a\n")
      assert {"a", _} = take_ingest()
      assert wait_until(fn -> health(opts, reader) == :healthy end, 2_000)
    end

    test "a file that does not grow is healthy", %{events: file} do
      File.write!(file, "a\n")
      {opts, reader} = start_reader(%{poll_interval_ms: 20})
      assert {"a", _} = take_ingest()

      since = :erlang.monotonic_time(:millisecond)
      assert wait_until(fn -> :sys.get_state(reader).last_settled_at > since + 200 end, 2_000)
      assert health(opts, reader) == :healthy
    end

    test "a reader whose last settled poll is older than the idle window is unhealthy",
         %{events: file} do
      File.write!(file, "")
      {opts, reader} = start_reader(%{idle_after_ms: 100})
      assert_committed(0)

      assert wait_until(
               fn ->
                 match?({:unhealthy, {:poll_idle, age}} when age > 100, health(opts, reader))
               end,
               2_000
             )
    end
  end

  # Against `FlezhaTest.Helper.FakeS3`, or MinIO under `S3_INTEGRATION=1`.
  describe "an object under an S3 root" do
    if Minio.integration?(), do: @describetag(:s3_integration)

    setup do
      Minio.plugin_bucket!("flezha-file-source-test")
    end

    test "an object's lines become frames in order", %{s3: s3} do
      :ok = S3.put("events.ndjson", "a\nb\n", [], s3)
      start_reader()

      for line <- ["a", "b"], do: assert({^line, %{"channel" => @channel}} = take_ingest())
      assert_committed(4)
    end

    test "a replaced object is read from the start", %{s3: s3} do
      :ok = S3.put("events.ndjson", "a\nb\n", [], s3)
      {_opts, reader} = start_reader()
      for line <- ["a", "b"], do: assert({^line, _} = take_ingest())
      assert_committed(4)

      # Longer than the offset, so only the ETag tells it from the one read.
      :ok = S3.put("events.ndjson", "cc\ndd\n", [], s3)
      send(reader, :poll)

      for line <- ["cc", "dd"], do: assert({^line, _} = take_ingest())
      assert_committed(6)
    end

    test "an object replaced during a read is read from the start", %{s3: s3} do
      :ok = S3.put("events.ndjson", "aa\nbb\n", [], s3)
      start_reader(%{read_chunk_bytes: 3})

      # Held unanswered: the reader still has the rest of the object to read
      # against the ETag it looked at before this line.
      assert_receive {:"$gen_call", from, {:ingest, "aa", _metadata}}, 2_000
      :ok = S3.put("events.ndjson", "cccc\ndd\n", [], s3)
      GenServer.reply(from, :ok)

      for line <- ["cccc", "dd"], do: assert({^line, _} = take_ingest())
    end

    test "a committed offset past the end of the object is read from the start", %{s3: s3} do
      :ok = S3.put("events.ndjson", "a\n", [], s3)
      start_reader(%{offset: 10})

      assert {"a", _} = take_ingest()
      assert_committed(2)
    end

    test "a plugin restart resumes after the last committed line", %{s3: s3} do
      :ok = S3.put("events.ndjson", "1\n2\n3\n", [], s3)
      start_reader(%{offset: 4})

      assert {"3", _} = take_ingest()
      assert_committed(6)
      refute_ingest()
    end

    test "a missing object is unhealthy, and recovers when it appears", %{s3: s3} do
      {opts, reader} = start_reader()
      missing = {:unhealthy, {:file_missing, opts.path}}
      assert wait_until(fn -> health(opts, reader) == missing end, 2_000)

      :ok = S3.put("events.ndjson", "a\n", [], s3)
      send(reader, :poll)

      assert {"a", _} = take_ingest()
      assert wait_until(fn -> health(opts, reader) == :healthy end, 2_000)
    end

    test "a forbidden request is a failure naming the status, and prints no secret",
         %{s3: s3, root: root} do
      :ok = S3.put("events.ndjson", "a\n", [], s3)
      System.put_env("FLEZHA_TEST_S3_SECRET", "s3cr3t-value")
      {opts, reader} = start_reader(%{offset: 0, failure_threshold: 1})
      forbidden = {:unhealthy, {:read_failed, {:stat, {:http, 403}}}}
      assert wait_until(fn -> health(opts, reader) == forbidden end, 5_000)

      on_exit(override_log_level(:warning))

      log =
        capture_log([level: :warning], fn ->
          send(reader, :poll)
          settle(reader)
        end)

      status = capture_io(fn -> Cli.call("Controller.status", "[]", []) end)

      assert log =~ "reason={:stat, {:http, 403}}"
      assert status =~ ~r/file_plugin_root: +#{Regex.escape(root)}\n/
      assert :sys.get_state(reader).offset == 0
      refute_received {:"$gen_cast", {:plugin_opt_persist, _, :offset, _}}

      for printed <- [log, status, inspect(health(opts, reader))],
          do: refute(printed =~ "s3cr3t-value")
    end
  end

  describe "attach-time refusals" do
    test "the chain-side key is refused as an option this provider does not offer" do
      assert {:error, {:unknown_options, "file_source", ["channel_out"]}} =
               ProviderPrepare.prepare(Source, %{
                 path: "events.ndjson",
                 channel_in: @channel,
                 channel_out: "alerts"
               })
    end

    test "naming neither channel spelling is refused naming both" do
      assert {:error, {:entry_channel_required, "file_source"}} =
               ProviderPrepare.prepare(Source, %{path: "events.ndjson"})
    end
  end

  # Prepared and started as the extension table does. The reader stops with its
  # host, which is this test process.
  defp start_reader(extra \\ %{}) do
    opts =
      Map.merge(
        %{
          path: "events.ndjson",
          channel_in: @channel,
          poll_interval_ms: 60_000,
          plugin_id: "file-source-#{System.unique_integer([:positive])}"
        },
        extra
      )

    {:ok, prepared, reader} = ProviderPrepare.prepare_and_activate(Source, opts)
    ProviderPrepare.await_stop_on_exit(reader)
    {prepared, reader}
  end

  defp assert_committed(offset),
    do: assert_receive({:"$gen_cast", {:plugin_opt_persist, _, :offset, ^offset}}, 2_000)

  # Every message sent to the reader before this has been handled.
  defp settle(reader), do: :sys.get_state(reader)

  defp health(opts, reader), do: Source.health(Map.put(opts, :owned_pid, reader))
end
