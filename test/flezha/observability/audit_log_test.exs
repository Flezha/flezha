defmodule Flezha.Observability.AuditLogTest do
  # async: false: writes Application env; a shared on-disk root; the observability registry.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  # Supervised AuditLog children log a benign "shut down abnormally (exit) normal"
  # on teardown; capture so a green run stays quiet (logs still print on failure).
  @moduletag :capture_log

  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.TestSinks

  alias Flezha.Observability.AuditLog.{
    Codec,
    Index,
    Reader,
    Retention,
    Rotation,
    Sink,
    Writer
  }

  alias Flezha.Service.AuditLog, as: AuditSvc
  alias Flezha.Service.Cli
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper}

  @moduletag :audit_log

  setup %{tmp: base_dir} do
    on_exit(&stop_audit_supervisor/0)
    {:ok, dir: base_dir}
  end

  # ---------- Codec ----------

  describe "Codec" do
    test "encodes a signal with stable field order" do
      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :enable_source,
          measurements: %{count: 1},
          metadata: %{caller_source: :cli, request_id: "req-1", source: :flezha_state}
        })

      line = Codec.encode(signal)
      assert String.ends_with?(line, "\n")
      keys = field_order(line)
      assert keys == ~w[ts_us ts_iso source name measurements metadata caller namespace]

      {:ok, decoded} = Codec.decode(line)
      assert decoded["source"] == "observability_service"
      assert decoded["name"] == "enable_source"
      caller = decoded["caller"]
      assert caller["source"] == "cli"
      assert caller["request_id"] == "req-1"
      # platform-authn-authz delta: while `:authn_authz, :enabled` is
      # `false` (the test default), the audit log writes the caller's
      # `source` atom into the `principal` column so existing rows
      # remain attributable.
      assert caller["principal"] == "cli"
    end

    test "ts_iso matches the documented regex" do
      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :enable_source,
          metadata: %{}
        })

      {:ok, decoded} = Codec.decode(Codec.encode(signal))

      assert Regex.match?(
               ~r/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/,
               decoded["ts_iso"]
             )
    end

    test "missing principal renders as null, not absent" do
      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :tick,
          metadata: %{}
        })

      line = Codec.encode(signal)
      assert line =~ ~s("principal":null)
    end

    test "a record written before caller_roles decodes and renders unchanged" do
      # The line an earlier release wrote: metadata is an open map, so a record
      # whose roles were never recorded reads as one that names none.
      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :relay_service,
            name: :stop,
            measurements: %{count: 1},
            metadata: %{caller_source: :cli, principal: "alice", relay: "o1"}
          })
        )

      assert {:ok, decoded} = Codec.decode(line)
      assert decoded["name"] == "stop"
      assert decoded["metadata"]["relay"] == "o1"
      refute Map.has_key?(decoded["metadata"], "caller_roles")
    end

    test "decode rejects non-JSON lines" do
      assert {:error, _} = Codec.decode("not json\n")
    end

    test "a metadata key of every shape renders as it did before the shared rule" do
      # The codec's own `stringify_key/1` was deleted onto
      # `Signal.metadata_text/1`. These are the renderings it produced, pinned
      # as literals so the deletion is proved free rather than assumed.
      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :key_shapes,
          metadata: %{
            :a_key => 1,
            "b key" => 1,
            42 => 1,
            1.5 => 1,
            {:x, :y} => 1,
            [1, 2, 3] => 1,
            ~c"cl" => 1,
            nil => 1
          }
        })

      {:ok, decoded} = Codec.decode(Codec.encode(signal))

      assert decoded["metadata"] |> Map.keys() |> Enum.sort() ==
               Enum.sort([
                 "a_key",
                 "b key",
                 "42",
                 "1.5",
                 "{:x, :y}",
                 "[1, 2, 3]",
                 ~s(~c"cl"),
                 "nil"
               ])
    end

    test "a non-UTF-8 binary in metadata is written, not raised on" do
      # The audit record is produced by the same call that writes it: a value
      # the typed rule lets through is a record that is never written at all.
      bad = <<255, 0, 254>>

      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :bad_binary,
          metadata: %{payload: bad}
        })

      {:ok, decoded} = Codec.decode(Codec.encode(signal))
      assert decoded["metadata"]["payload"] == Signal.metadata_text(bad)
    end

    test "a key past inspect's default limit is rendered whole, not elided" do
      # The shared rule inspects with `limit: :infinity, printable_limit:
      # :infinity`; the deleted `stringify_key/1` used bare `inspect/1`, whose
      # default `:limit` is 200. This is the one deliberate widening the move
      # carries into the audit log — a long key reads complete instead of
      # ending in "...".
      long = Enum.to_list(1..250)

      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :long_key,
          metadata: %{long => 1}
        })

      {:ok, decoded} = Codec.decode(Codec.encode(signal))
      [rendered] = Map.keys(decoded["metadata"])

      assert rendered == inspect(long, limit: :infinity, printable_limit: :infinity)
      refute rendered =~ "..."
    end
  end

  # ---------- Index ----------

  describe "Index" do
    test "load and write round-trip", %{dir: dir} do
      entries = [
        %{
          filename: "audit-20260518T000000Z-1.jsonl",
          first_ts_us: 1,
          last_ts_us: 2,
          record_count: 3,
          size_bytes: 100,
          sha256: "abc"
        }
      ]

      :ok = Index.write(dir, entries)
      assert {:ok, ^entries} = Index.load(dir)
    end

    test "rebuild walks disk and recovers catalog", %{dir: dir} do
      # Drop a synthetic rotated file
      filename = "audit-20260518T000000Z-1.jsonl"

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      File.write!(Path.join(dir, filename), line)

      {:ok, entries} = Index.rebuild(dir)
      assert [%{filename: ^filename, record_count: 1}] = entries
    end

    test "rebuild tolerates a partial last line", %{dir: dir} do
      filename = "audit-20260518T000000Z-2.jsonl"

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      File.write!(Path.join(dir, filename), line <> ~s({"partial":))

      {:ok, [entry]} = Index.rebuild(dir)
      assert entry.record_count == 1
    end
  end

  # ---------- Reader ----------

  describe "Reader" do
    test "filter pushdown by source, name, limit", %{dir: dir} do
      file = Path.join(dir, "audit-test.jsonl")

      records =
        for i <- 1..5 do
          src = if rem(i, 2) == 0, do: :observability_service, else: :flezha_shutdown

          Codec.encode(
            Signal.new(%{
              kind: :event,
              source: src,
              name: :tick,
              measurements: %{count: i},
              metadata: %{}
            })
          )
        end

      File.write!(file, IO.iodata_to_binary(records))

      filtered = Reader.stream(file, source: "observability_service") |> Enum.to_list()
      assert length(filtered) == 2

      limited = Reader.stream(file, limit: 3) |> Enum.to_list()
      assert length(limited) == 3
    end

    test "reads gzipped file transparently", %{dir: dir} do
      file = Path.join(dir, "audit-gz.jsonl.gz")

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :enable_source,
            metadata: %{}
          })
        )

      gz = :zlib.gzip(line)
      File.write!(file, gz)

      records = Reader.stream(file, []) |> Enum.to_list()
      assert length(records) == 1
      assert hd(records)["name"] == "enable_source"
    end

    test "skips truncated last line", %{dir: dir} do
      file = Path.join(dir, "audit-trunc.jsonl")

      good =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      File.write!(file, good <> ~s({"truncated":))

      records = Reader.stream(file, []) |> Enum.to_list()
      assert length(records) == 1
    end
  end

  # ---------- flz audit tail ----------

  describe "flz audit tail" do
    test "prints each record, and ends when the writer closes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      tail =
        Task.async(fn ->
          ExUnit.CaptureIO.capture_io(fn ->
            Cli.call("AuditLog.tail", "[]", output: "json", duration_ms: 60_000)
          end)
        end)

      assert NodeTestHelper.wait_until(fn -> :sys.get_state(Writer).subscribers != %{} end, 5_000)

      :ok =
        Writer.write(Codec.encode(Signal.new(%{kind: :event, source: :audit_log, name: :tailed})))

      :ok = Writer.close()

      assert Task.await(tail, 5_000) =~ "tailed"
    end

    test "unsubscribes when its window closes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("AuditLog.tail", "[]", duration_ms: 10)
      end)

      assert :sys.get_state(Writer).subscribers == %{}
    end
  end

  # ---------- a tail forwarded to a pid ----------

  # The `:erpc` worker a consumer on another node reaches runs the tail and
  # hands each record to the pid the consumer names.
  describe "a tail forwarded to a pid" do
    test "hands each record on and releases the subscription when the pid exits", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      {sink, tail} = forwarded_tail(%{source: :ui})

      :ok = Writer.write(Codec.encode(Signal.new(%{kind: :event, source: :t, name: :forwarded})))
      assert_receive {:forwarded, %{"name" => "forwarded"}}, 2_000

      Process.exit(sink, :kill)
      assert Task.await(tail, 2_000) == {:ok, :caller_down}
      assert :sys.get_state(Writer).subscribers == %{}
    end

    test "ends when the writer closes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      {_sink, tail} = forwarded_tail(%{source: :ui})

      :ok = Writer.close()
      assert Task.await(tail, 2_000) == {:ok, :done}
      assert :sys.get_state(Writer).subscribers == %{}
    end

    test "drops a record of a namespace the caller may not read", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      AuthTestHelper.setup_auth_stores(prefix: "flz_audit_tail", claim: true)
      context = %{source: :ui, principal: "viewer@team-a", namespace: "team-a", role: :viewer}
      {sink, tail} = forwarded_tail(context)

      for {name, namespace} <- [other: "b", own: "team-a"] do
        signal =
          Signal.new(%{kind: :event, source: :t, name: name, metadata: %{namespace: namespace}})

        :ok = Writer.write(Codec.encode(signal))
      end

      assert_receive {:forwarded, %{"name" => "own"}}, 2_000
      refute_received {:forwarded, %{"name" => "other"}}

      Process.exit(sink, :kill)
      assert {:ok, :caller_down} = Task.await(tail, 2_000)
    end
  end

  # ---------- Writer ----------

  describe "Writer" do
    test "per-record fsync writes and rotates", %{dir: dir} do
      configure(dir, fsync_mode: :per_record)
      {:ok, _} = start_audit_supervisor()

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      assert :ok = Writer.write(line)

      stats = Writer.stats()
      assert stats.current_file_bytes == byte_size(line)
      assert stats.current_record_count == 1

      {:ok, tail} = Writer.subscribe_tail()
      {:ok, info} = Writer.rotate_now(System.system_time(:microsecond))
      assert String.starts_with?(info.rotated_to, "audit-")
      assert info.size_bytes == byte_size(line)

      # The rotation's own event, sent after the reply, is the new file's first record.
      assert_receive {:audit_record, ^tail, %{"name" => "audit_rotated"}}, 2_000
      stats2 = Writer.stats()
      assert stats2.current_record_count == 1
      assert stats2.catalog_count == 1
    end

    test "double rotation within the same second produces -1 then -2", %{dir: dir} do
      configure(dir, fsync_mode: :per_record)
      {:ok, _} = start_audit_supervisor()

      ts_us = System.system_time(:microsecond)

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      :ok = Writer.write(line)
      {:ok, info1} = Writer.rotate_now(ts_us)

      :ok = Writer.write(line)
      {:ok, info2} = Writer.rotate_now(ts_us)

      assert String.ends_with?(info1.rotated_to, "-1.jsonl")
      assert String.ends_with?(info2.rotated_to, "-2.jsonl")
    end

    test "close drains and rejects further writes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      line =
        Codec.encode(
          Signal.new(%{
            kind: :event,
            source: :observability_service,
            name: :tick,
            metadata: %{}
          })
        )

      :ok = Writer.write(line)
      :ok = Writer.close()

      assert Writer.closed?()
      assert {:error, :closed} = Writer.write(line)
    end
  end

  # ---------- Sink + Bus integration ----------

  describe "Sink end-to-end" do
    test "default allow-list captures observability_service events", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      AuditSvc.list_files(%{source: :cli})
      TestSinks.settle(Sink)

      # The list_files call itself emits a :list_files event on :audit_log
      # source — covered by the audit-of-audit test below. The
      # cross-source default we want to test is :observability_service.
      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :disable_source,
          measurements: %{count: 1},
          metadata: %{caller_source: :cli, source: :flezha_state}
        })
      )

      TestSinks.settle(Sink)

      path = Path.join(dir, "audit-current.jsonl")
      content = File.read!(path)

      assert content =~ ~s("source":"observability_service")
      assert content =~ ~s("name":"disable_source")

      decoded =
        content
        |> String.split("\n", trim: true)
        |> Enum.map(fn line ->
          {:ok, r} = Codec.decode(line)
          r
        end)

      observed = Enum.find(decoded, &(&1["source"] == "observability_service"))
      assert observed["caller"]["source"] == "cli"
    end

    test "non-allow-listed signals are discarded", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      registered_source =
        :"audit_drop_#{:erlang.unique_integer([:positive])}"

      :ok =
        Flezha.Observability.Registry.register_source(%{
          name: registered_source,
          kinds: [:event],
          flavour: :declarative,
          cardinality_budget: 10
        })

      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: registered_source,
          name: :ignored,
          measurements: %{count: 1},
          metadata: %{}
        })
      )

      TestSinks.settle(Sink)

      path = Path.join(dir, "audit-current.jsonl")

      content =
        case File.read(path) do
          {:ok, c} -> c
          _ -> ""
        end

      refute content =~ Atom.to_string(registered_source)
    end

    test "extra subscriptions are honoured", %{dir: dir} do
      extra_source =
        :"audit_extra_#{:erlang.unique_integer([:positive])}"

      configure(dir, extra_subscriptions: [{extra_source, :any}])
      {:ok, _} = start_audit_supervisor()

      :ok =
        Flezha.Observability.Registry.register_source(%{
          name: extra_source,
          kinds: [:event],
          flavour: :declarative,
          cardinality_budget: 10
        })

      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: extra_source,
          name: :critical,
          measurements: %{count: 1},
          metadata: %{}
        })
      )

      TestSinks.settle(Sink)

      content = File.read!(Path.join(dir, "audit-current.jsonl"))
      assert content =~ Atom.to_string(extra_source)
    end

    test "post-close drops increment the counter", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      :ok = Writer.close()

      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :disable_source,
          measurements: %{count: 1},
          metadata: %{caller_source: :cli}
        })
      )

      TestSinks.settle(Sink)
      assert Sink.post_close_dropped() >= 1
    end
  end

  # ---------- Retention ----------

  describe "Retention" do
    test "age-based deletion removes stale files", %{dir: dir} do
      configure(dir, retention_seconds: 1)
      {:ok, _} = start_audit_supervisor()

      stale = Path.join(dir, "audit-20260101T000000Z-1.jsonl")
      File.write!(stale, "")
      File.touch!(stale, {{2020, 1, 1}, {0, 0, 0}})

      Index.write(dir, [
        %{
          filename: Path.basename(stale),
          first_ts_us: 0,
          last_ts_us: 0,
          record_count: 0,
          size_bytes: 0,
          sha256: nil
        }
      ])

      {:ok, result} = Retention.sweep_now()
      assert result.deleted_count >= 1
      refute File.exists?(stale)
    end

    test "active file never deleted", %{dir: dir} do
      configure(dir, retention_seconds: 0)
      {:ok, _} = start_audit_supervisor()

      active = Path.join(dir, "audit-current.jsonl")

      {:ok, _} = Retention.sweep_now()
      assert File.exists?(active)
    end
  end

  # ---------- Service API ----------

  describe "Service API" do
    test "list_files returns catalog and audits the read", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      # Generate one rotated file
      :ok =
        Writer.write(
          Codec.encode(
            Signal.new(%{
              kind: :event,
              source: :observability_service,
              name: :tick,
              metadata: %{}
            })
          )
        )

      {:ok, _} = Writer.rotate_now()

      {:ok, entries} = AuditSvc.list_files(%{source: :cli})
      assert length(entries) == 1

      TestSinks.settle(Sink)
      content = File.read!(Path.join(dir, "audit-current.jsonl"))
      assert content =~ ~s("source":"audit_log")
      assert content =~ ~s("name":"list_files")
    end

    # Fails when: a name that walks out of the audit directory is joined onto
    # it, and the answer tells a caller whether a file exists on the host.
    test "read answers a name outside the audit directory as missing", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      outside = Path.join(Path.dirname(dir), "outside.jsonl")
      File.write!(outside, "")
      on_exit(fn -> File.rm(outside) end)

      for file <- ["../outside.jsonl", outside, ".."] do
        assert {:error, %Flezha.Error{code: "audit.file_not_found"}} =
                 AuditSvc.read([file: file], %{source: :internal})
      end
    end

    test "invalid caller_context is rejected", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               AuditSvc.list_files(%{})

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} = AuditSvc.stats(%{})
    end

    test "audit_log_disabled returned when subsystem off", %{dir: dir} do
      configure(dir, enabled: false)

      assert {:error, %Flezha.Error{code: "audit.disabled"}} =
               AuditSvc.list_files(%{source: :cli})

      assert {:error, %Flezha.Error{code: "audit.disabled"}} = AuditSvc.stats(%{source: :cli})
    end

    test "rotate forces immediate rotation", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      :ok =
        Writer.write(
          Codec.encode(
            Signal.new(%{
              kind: :event,
              source: :observability_service,
              name: :tick,
              metadata: %{}
            })
          )
        )

      {:ok, info} = AuditSvc.rotate(%{source: :cli})
      assert String.starts_with?(info.rotated_to, "audit-")
    end
  end

  # ---------- Helpers ----------

  # A tail held by an `:erpc` worker for a sink process that forwards what it
  # is handed to the test; answers once the worker has subscribed.
  defp forwarded_tail(context) do
    test = self()
    sink = spawn(fn -> forward_to(test) end)

    tail =
      Task.async(fn ->
        :erpc.call(node(), AuditSvc, :tail, [[], Map.put(context, :emit, sink)])
      end)

    assert NodeTestHelper.wait_until(fn -> :sys.get_state(Writer).subscribers != %{} end, 5_000)
    {sink, tail}
  end

  defp forward_to(test) do
    receive do
      {:flezha_emit, record} -> send(test, {:forwarded, record})
    end

    forward_to(test)
  end

  defp default_config() do
    [
      enabled: false,
      dir: "app_data/audit",
      max_file_bytes: 67_108_864,
      max_file_age_seconds: 86_400,
      retention_seconds: 30 * 86_400,
      max_total_bytes: 4_294_967_296,
      fsync_mode: :per_record,
      fsync_interval_ms: 100,
      rotation_check_interval_ms: 60_000,
      retention_check_interval_ms: 3_600_000,
      extra_subscriptions: []
    ]
  end

  defp configure(dir, overrides \\ []) do
    base = default_config() |> Keyword.put(:enabled, true) |> Keyword.put(:dir, dir)
    Application.put_env(:flezha, :audit_log, Keyword.merge(base, overrides))
  end

  defp start_audit_supervisor() do
    stop_audit_supervisor()

    _ =
      Flezha.Observability.Registry.register_source(
        Flezha.Observability.AuditLog.Supervisor.__source__()
      )

    children = [Writer, Rotation, Retention, Sink]

    {:ok, sup} = Elixir.Supervisor.start_link(children, strategy: :one_for_one)
    Process.put({__MODULE__, :sup}, sup)
    {:ok, sup}
  end

  defp stop_audit_supervisor() do
    # Stop the supervisor itself, not the children by name: stopping a permanent
    # child under a live :one_for_one supervisor just makes it restart, and the
    # lingering supervisor's later teardown logs "shut down abnormally (exit)
    # normal". Supervisor.stop terminates the tree in order with :shutdown.
    case Process.delete({__MODULE__, :sup}) do
      pid when is_pid(pid) ->
        if Process.alive?(pid), do: Elixir.Supervisor.stop(pid, :normal, 2_000)

      _ ->
        :ok
    end

    # Free any orphaned named children left by a crashed prior run.
    for mod <- [Sink, Retention, Rotation, Writer] do
      case Process.whereis(mod) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid, :shutdown, 1_000)
          catch
            _, _ -> :ok
          end
      end
    end

    _ = Flezha.Observability.Registry.unregister_sink(:audit_log)
  end

  defp field_order(line) do
    # Use Jason's `keys: :strings` to keep order. Since Jason returns a
    # map (unordered), we instead parse the binary directly to derive
    # the top-level field order without using regex backtracking.
    line = String.trim_trailing(line, "\n")
    extract_top_level_keys(line)
  end

  defp extract_top_level_keys("{" <> rest) do
    extract_keys(rest, 0, [], "")
  end

  defp extract_keys("", _depth, keys, _buf), do: Enum.reverse(keys)

  defp extract_keys("\\" <> <<_::utf8, rest::binary>>, depth, keys, buf),
    do: extract_keys(rest, depth, keys, buf)

  defp extract_keys("\"" <> rest, 0, keys, "") do
    {key, rest2} = read_string(rest, "")
    rest3 = skip_until_value(rest2)
    rest4 = skip_value(rest3)

    rest5 =
      case rest4 do
        "," <> r -> r
        "}" <> _ -> ""
        _ -> rest4
      end

    extract_keys(rest5, 0, [key | keys], "")
  end

  defp extract_keys(<<_::utf8, rest::binary>>, depth, keys, buf),
    do: extract_keys(rest, depth, keys, buf)

  defp read_string("\"" <> rest, acc), do: {acc, rest}

  defp read_string(<<c::utf8, rest::binary>>, acc),
    do: read_string(rest, acc <> <<c::utf8>>)

  defp skip_until_value(":" <> rest), do: rest
  defp skip_until_value(<<_::utf8, rest::binary>>), do: skip_until_value(rest)
  defp skip_until_value(""), do: ""

  defp skip_value(s), do: skip_value(s, 0, false)

  defp skip_value("", _depth, _in_string), do: ""
  defp skip_value("," <> _ = s, 0, false), do: s
  defp skip_value("}" <> _ = s, 0, false), do: s

  defp skip_value("\"" <> rest, depth, in_string) do
    skip_value(rest, depth, not in_string)
  end

  defp skip_value("\\" <> <<_::utf8, rest::binary>>, depth, true),
    do: skip_value(rest, depth, true)

  defp skip_value("{" <> rest, depth, false), do: skip_value(rest, depth + 1, false)
  defp skip_value("[" <> rest, depth, false), do: skip_value(rest, depth + 1, false)

  defp skip_value("}" <> rest, depth, false) when depth > 0,
    do: skip_value(rest, depth - 1, false)

  defp skip_value("]" <> rest, depth, false) when depth > 0,
    do: skip_value(rest, depth - 1, false)

  defp skip_value(<<_::utf8, rest::binary>>, depth, in_string),
    do: skip_value(rest, depth, in_string)
end
