defmodule Flezha.Logs.AuditKindTest do
  # async: false: writes Application env; a shared on-disk root; the observability registry.
  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  # The supervised kind children log a benign "shut down abnormally (exit) normal"
  # on teardown; capture so a green run stays quiet (logs still print on failure).
  @moduletag :capture_log

  alias Flezha.Logs.{Codec, Index, Kinds, Reader, Retention, Rotation, Sink, Writer}
  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.Cli
  alias Flezha.Service.Logs
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper}

  @moduletag :audit_log

  @pubsub Flezha.Observability.PubSub

  setup %{tmp: base_dir} do
    on_exit(fn -> Flezha.Observability.Registry.unregister_sink(:audit_log) end)
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

      line = encode(signal)
      assert String.ends_with?(line, "\n")
      keys = field_order(line)
      assert keys == ~w[ts_us ts_iso source name measurements metadata caller namespace node]

      {:ok, decoded} = Codec.decode(line)
      assert decoded["source"] == "observability_service"
      assert decoded["name"] == "enable_source"
      assert decoded["node"] == Atom.to_string(node())
      caller = decoded["caller"]
      assert caller["source"] == "cli"
      assert caller["request_id"] == "req-1"
      # While `:authn_authz, :enabled` is `false` (the test default), the audit
      # log writes the caller's `source` atom into the `principal` column so
      # rows remain attributable.
      assert caller["principal"] == "cli"
    end

    test "ts_us is wall-clock and ts_iso names the same instant" do
      before_us = System.system_time(:microsecond)

      {:ok, decoded} =
        Codec.decode(encode(Signal.new(%{kind: :event, source: :t, name: :tick, metadata: %{}})))

      assert decoded["ts_us"] >= before_us - 1_000_000
      assert decoded["ts_us"] <= System.system_time(:microsecond)
      assert Codec.order_us(decoded) == decoded["ts_us"]
      assert decoded["ts_iso"] == Codec.iso(decoded["ts_us"])
    end

    test "ts_iso matches the documented regex" do
      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :enable_source,
          metadata: %{}
        })

      {:ok, decoded} = Codec.decode(encode(signal))

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

      line = encode(signal)
      assert line =~ ~s("principal":null)
    end

    test "a record written before caller_roles decodes and renders unchanged" do
      # The line an earlier release wrote: metadata is an open map, so a record
      # whose roles were never recorded reads as one that names none.
      line =
        encode(
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

    test "a record written before node orders by its ts_iso" do
      # Such a record carried the writer's monotonic time in `ts_us`.
      old = %{"ts_us" => -576_460_000_000, "ts_iso" => "2026-05-18T00:00:00.000Z"}

      assert Codec.order_us(old) ==
               DateTime.to_unix(~U[2026-05-18 00:00:00.000Z], :microsecond)
    end

    test "decode rejects non-JSON lines" do
      assert {:error, _} = Codec.decode("not json\n")
    end

    test "a metadata key of every shape renders as it did before the shared rule" do
      # The renderings a metadata key of each shape produces, pinned as literals.
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

      {:ok, decoded} = Codec.decode(encode(signal))

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

      {:ok, decoded} = Codec.decode(encode(signal))
      assert decoded["metadata"]["payload"] == Signal.metadata_text(bad)
    end

    test "a key past inspect's default limit is rendered whole, not elided" do
      # The shared rule inspects with `limit: :infinity, printable_limit:
      # :infinity`, so a long key reads complete instead of ending in "...".
      long = Enum.to_list(1..250)

      signal =
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :long_key,
          metadata: %{long => 1}
        })

      {:ok, decoded} = Codec.decode(encode(signal))
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
          sha256: "abc",
          clock: :wall
        }
      ]

      :ok = Index.write(dir, entries)
      assert {:ok, ^entries} = Index.load(dir)
    end

    test "rebuild walks disk and recovers catalog", %{dir: dir} do
      filename = "audit-20260518T000000Z-1.jsonl"
      line = tick()
      File.write!(Path.join(dir, filename), line)
      {:ok, decoded} = Codec.decode(line)

      {:ok, entries} = Index.rebuild(kind(), dir)

      assert [%{filename: ^filename, record_count: 1, clock: :wall, first_ts_us: ts}] = entries
      assert ts == decoded["ts_us"]
    end

    test "rebuild tolerates a partial last line", %{dir: dir} do
      filename = "audit-20260518T000000Z-2.jsonl"
      File.write!(Path.join(dir, filename), tick() <> ~s({"partial":))

      {:ok, [entry]} = Index.rebuild(kind(), dir)
      assert entry.record_count == 1
    end

    test "only the kind's rotated files are catalogued", %{dir: dir} do
      for name <- ["audit-current.jsonl", "relay_history-20260518T000000Z-1.jsonl", "INDEX"],
          do: File.write!(Path.join(dir, name), tick())

      File.write!(Path.join(dir, "audit-20260518T000000Z-1.jsonl.gz"), :zlib.gzip(tick()))

      assert Index.list_rotated_files(kind(), dir) == ["audit-20260518T000000Z-1.jsonl.gz"]
    end
  end

  # ---------- Reader ----------

  describe "Reader" do
    test "filter pushdown by source, name, limit", %{dir: dir} do
      file = Path.join(dir, "audit-test.jsonl")

      records =
        for i <- 1..5 do
          src = if rem(i, 2) == 0, do: :observability_service, else: :flezha_shutdown

          encode(
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

      by_source = Reader.matcher(%{"source" => "observability_service"}, nil, nil)
      assert file |> Reader.stream(match: by_source) |> Enum.count() == 2

      by_name = Reader.matcher(%{source: "flezha_shutdown", name: "tick"}, nil, nil)
      assert file |> Reader.stream(match: by_name) |> Enum.count() == 3

      limited = Reader.stream(file, limit: 3) |> Enum.to_list()
      assert length(limited) == 3
    end

    test "matcher narrows by the record's time window", %{dir: dir} do
      file = Path.join(dir, "audit-window.jsonl")
      File.write!(file, tick() <> tick())
      [first, second] = file |> Reader.stream() |> Enum.map(&Codec.order_us/1)

      assert file |> Reader.stream(match: Reader.matcher(%{}, second, nil)) |> Enum.count() ==
               Enum.count([first, second], &(&1 >= second))

      assert file |> Reader.stream(match: Reader.matcher(%{}, nil, first - 1)) |> Enum.count() ==
               0
    end

    test "reverse_stream reads newest first with byte offsets", %{dir: dir} do
      file = Path.join(dir, "audit-reverse.jsonl")

      lines =
        for name <- [:a, :b, :c],
            do: encode(Signal.new(%{kind: :event, source: :t, name: name, metadata: %{}}))

      File.write!(file, IO.iodata_to_binary(lines) <> ~s({"truncated":))

      assert [{o3, %{"name" => "c"}}, {o2, %{"name" => "b"}}, {0, %{"name" => "a"}}] =
               file |> Reader.reverse_stream() |> Enum.to_list()

      assert o2 == byte_size(Enum.at(lines, 0))
      assert o3 == o2 + byte_size(Enum.at(lines, 1))
    end

    test "reads gzipped file transparently", %{dir: dir} do
      file = Path.join(dir, "audit-gz.jsonl.gz")

      line =
        encode(
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
      File.write!(file, tick() <> ~s({"truncated":))

      records = Reader.stream(file, []) |> Enum.to_list()
      assert length(records) == 1
    end
  end

  # ---------- flz audit tail ----------

  describe "flz audit tail" do
    test "prints each record written while its window is open", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      tail =
        Task.async(fn ->
          ExUnit.CaptureIO.capture_io(fn ->
            Cli.call("Logs.tail", ~s([{"kind":"audit"}]), output: "json", duration_ms: 1_000)
          end)
        end)

      assert NodeTestHelper.wait_until(&tail_subscribed?/0, 5_000)

      :ok = Writer.append(kind(), encode(Signal.new(%{kind: :event, source: :t, name: :tailed})))

      assert Task.await(tail, 5_000) =~ "tailed"
    end

    test "unsubscribes when its window closes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Logs.tail", ~s([{"kind":"audit"}]), duration_ms: 10)
      end)

      refute tail_subscribed?()
    end
  end

  # ---------- a tail forwarded to a pid ----------

  # The HTTP stream route runs the tail in a task, which hands each record the
  # writer broadcasts to the pid the context names as `:emit`.
  describe "a tail forwarded to a pid" do
    test "hands each record on and releases the subscription when the pid exits", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      {sink, tail} = forwarded_tail(%{source: :http})

      :ok =
        Writer.append(kind(), encode(Signal.new(%{kind: :event, source: :t, name: :forwarded})))

      assert_receive {:forwarded, %{"name" => "forwarded", "node" => node_name}}, 2_000
      assert node_name == Atom.to_string(node())

      Process.exit(sink, :kill)
      assert Task.await(tail, 2_000) == {:ok, :caller_down}
      refute tail_subscribed?()
    end

    test "drops a record of a namespace the caller may not read", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      AuthTestHelper.setup_auth_stores(prefix: "flz_audit_tail", claim: true)
      context = %{source: :http, principal: "viewer@team-a", namespace: "team-a", role: :viewer}
      {sink, tail} = forwarded_tail(context)

      for {name, namespace} <- [other: "b", own: "team-a"] do
        signal =
          Signal.new(%{kind: :event, source: :t, name: name, metadata: %{namespace: namespace}})

        :ok = Writer.append(kind(), encode(signal))
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

      line = tick()
      assert :ok = Writer.append(kind(), line)

      stats = Writer.stats(kind())
      assert stats.current_file_bytes == byte_size(line)
      assert stats.current_record_count == 1

      :ok = Phoenix.PubSub.subscribe(@pubsub, Writer.topic(kind()))
      {:ok, info} = Writer.rotate_now(kind(), System.system_time(:microsecond))
      assert String.starts_with?(info.rotated_to, "audit-")
      assert info.size_bytes == byte_size(line)

      # The rotation's own event, sent after the reply, is the new file's first record.
      assert_broadcast_line("audit_rotated")
      stats2 = Writer.stats(kind())
      assert stats2.current_record_count == 1
      assert stats2.catalog_count == 1
      assert [%{clock: :wall, record_count: 1}] = Writer.catalog(kind())
    end

    test "double rotation within the same second produces -1 then -2", %{dir: dir} do
      configure(dir, fsync_mode: :per_record)
      {:ok, _} = start_audit_supervisor()

      ts_us = System.system_time(:microsecond)
      line = tick()

      :ok = Writer.append(kind(), line)
      {:ok, info1} = Writer.rotate_now(kind(), ts_us)

      :ok = Writer.append(kind(), line)
      {:ok, info2} = Writer.rotate_now(kind(), ts_us)

      assert String.ends_with?(info1.rotated_to, "-1.jsonl")
      assert String.ends_with?(info2.rotated_to, "-2.jsonl")
    end

    test "close drains and rejects further writes", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      line = tick()
      :ok = Writer.append(kind(), line)
      :ok = Writer.close(kind())

      assert Writer.closed?(kind())
      assert {:error, :closed} = Writer.append(kind(), line)
      assert Writer.counters(kind()).post_close_dropped == 1
    end
  end

  # ---------- Sink + Bus integration ----------

  describe "Sink end-to-end" do
    test "default allow-list captures observability_service events", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      Logs.files(%{kind: "audit"}, %{source: :cli})
      TestSinks.settle(Sink)

      # The files call itself writes a :files record on the :audit_log
      # source, covered by the Service API test below. The cross-source
      # default tested here is :observability_service.
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

    test "the bus filter names the allow-list's sources" do
      sources = Sink.filter().sources
      assert :platform_authz in sources
      assert :audit_log in sources
      assert sources == Enum.uniq(sources)
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

      :ok = Writer.close(kind())

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
      assert Writer.counters(kind()).post_close_dropped >= 1
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

      {:ok, result} = Retention.sweep_now(kind())
      assert result.deleted_count >= 1
      refute File.exists?(stale)
      assert {:ok, []} = Index.load(dir)
    end

    test "active file never deleted", %{dir: dir} do
      configure(dir, retention_seconds: 0)
      {:ok, _} = start_audit_supervisor()

      active = Path.join(dir, "audit-current.jsonl")

      {:ok, _} = Retention.sweep_now(kind())
      assert File.exists?(active)
    end
  end

  # ---------- Service API ----------

  describe "Service API" do
    test "files returns catalog and audits the read", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      :ok = Writer.append(kind(), tick())
      {:ok, _} = Writer.rotate_now(kind())

      {:ok, %{files: entries, unreachable: []}} = Logs.files(%{kind: "audit"}, %{source: :cli})
      this_node = node()
      assert [%{node: ^this_node, record_count: 1}] = entries

      TestSinks.settle(Sink)
      content = File.read!(Path.join(dir, "audit-current.jsonl"))
      assert content =~ ~s("source":"audit_log")
      assert content =~ ~s("name":"files")
    end

    test "read pages the newest records first", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      for name <- [:first, :second, :third],
          do:
            :ok =
              Writer.append(
                kind(),
                encode(Signal.new(%{kind: :event, source: :t, name: name, metadata: %{}}))
              )

      {:ok, _} = Writer.rotate_now(kind())
      filters = %{source: "t"}

      assert {:ok, %{records: [%{"name" => "third"}, %{"name" => "second"}], next: next}} =
               Logs.read(%{kind: "audit", filters: filters, limit: 2}, %{source: :internal})

      assert {:ok, %{records: [%{"name" => "first"}], next: nil}} =
               Logs.read(%{kind: "audit", filters: filters, limit: 2, before: next}, %{
                 source: :internal
               })
    end

    test "a record written by a console of source ui reads back as written", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      line =
        encode(
          Signal.new(%{
            kind: :event,
            source: :relay_service,
            name: :stop,
            metadata: %{caller_source: :ui, principal: "console@_system", relay: "o1"}
          })
        )

      :ok = Writer.append(kind(), line)
      {:ok, _} = Writer.rotate_now(kind())
      {:ok, written} = Codec.decode(line)
      admin = %{source: :http, principal: "admin@_system", namespace: "_system", role: :admin}

      assert {:ok, %{records: [^written]}} =
               Logs.read(%{kind: "audit", filters: %{source: "relay_service"}}, admin)
    end

    test "read refuses a filter the kind does not declare", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      assert {:error, %Flezha.Error{}} =
               Logs.read(%{kind: "audit", filters: %{relay: "r"}}, %{source: :internal})
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
                 Logs.read(%{kind: "audit", file: file}, %{source: :internal})
      end
    end

    test "invalid caller_context is rejected", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Logs.files(%{kind: "audit"}, %{})

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Logs.stats(%{kind: "audit"}, %{})
    end

    test "audit_log_disabled returned when subsystem off", %{dir: dir} do
      configure(dir, enabled: false)

      assert {:error, %Flezha.Error{code: "audit.disabled"}} =
               Logs.files(%{kind: "audit"}, %{source: :cli})

      assert {:error, %Flezha.Error{code: "audit.disabled"}} =
               Logs.stats(%{kind: "audit"}, %{source: :cli})
    end

    test "stats answers one row per controller", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()
      :ok = Writer.append(kind(), tick())

      this_node = node()

      assert {:ok, %{controllers: [%{node: ^this_node, record_count: n}], unreachable: []}} =
               Logs.stats(%{kind: "audit"}, %{source: :cli})

      assert n >= 1
    end

    test "rotate forces immediate rotation", %{dir: dir} do
      configure(dir)
      {:ok, _} = start_audit_supervisor()

      :ok = Writer.append(kind(), tick())

      {:ok, %{rotated: [%{node: node_name, rotated_to: rotated_to}], unreachable: []}} =
        Logs.rotate(%{kind: "audit"}, %{source: :cli})

      assert node_name == node()
      assert String.starts_with?(rotated_to, "audit-")
    end
  end

  # ---------- Helpers ----------

  defp kind(), do: Kinds.fetch!(:audit)

  defp encode(signal), do: Codec.encode(kind(), signal)

  defp tick(),
    do:
      encode(
        Signal.new(%{
          kind: :event,
          source: :observability_service,
          name: :tick,
          metadata: %{}
        })
      )

  defp tail_subscribed?(), do: Registry.lookup(@pubsub, Writer.topic(kind())) != []

  defp assert_broadcast_line(fragment) do
    assert_receive {:log_lines, :audit, lines}, 2_000
    unless Enum.any?(lines, &(&1 =~ fragment)), do: assert_broadcast_line(fragment)
  end

  # A tail held by a task for a sink process that forwards what it is handed
  # to the test; answers once the task has subscribed.
  defp forwarded_tail(context) do
    test = self()
    sink = spawn(fn -> forward_to(test) end)

    tail =
      Task.async(fn -> Logs.tail(%{kind: "audit"}, Map.put(context, :emit, sink)) end)

    assert NodeTestHelper.wait_until(&tail_subscribed?/0, 5_000)
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
      dir: "storage/app/audit",
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

  # The audit kind is off in the test config, so `Flezha.Logs.Supervisor`
  # started no subtree for it: the test starts the one it would. ExUnit owns
  # the tree, so it is stopped in order before `on_exit` runs.
  defp start_audit_supervisor() do
    _ = Flezha.Observability.Registry.register_source(Flezha.Logs.Sink.__source__())

    kind = kind()
    children = [{Writer, kind}, {Rotation, kind}, {Retention, kind}, Sink]

    sup =
      start_supervised!(%{
        id: :audit_sup,
        start: {Elixir.Supervisor, :start_link, [children, [strategy: :one_for_one]]},
        type: :supervisor
      })

    {:ok, sup}
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
