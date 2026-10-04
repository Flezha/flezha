defmodule Flezha.Service.ObservabilityTest do
  # async: false: writes Application env; the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.AuditLog.Codec
  alias Flezha.Observability.Bus
  alias Flezha.Observability.{PubSubSink, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.{Admin, Cli, Observability}

  setup do
    src = :"obs_svc_test_src_#{:erlang.unique_integer([:positive])}"
    :ok = Registry.register_source(%{name: src, kinds: [:event], flavour: :declarative})
    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src}
  end

  describe "caller_context validation" do
    test "rejects missing :source field" do
      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.list_sources(%{})
    end

    test "rejects invalid :source value" do
      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.list_sources(%{source: :weird_thing})
    end

    test "accepts each of :cli, :ui, :sdk, :internal" do
      for source <- [:cli, :ui, :sdk, :internal] do
        assert {:ok, _} = Observability.list_sources(%{source: source})
      end
    end
  end

  describe "list_sources / list_sinks" do
    test "lists from Registry", %{src: src} do
      {:ok, sources} = Observability.list_sources(%{source: :internal})
      assert Enum.any?(sources, &(&1.name == src))

      {:ok, sinks} = Observability.list_sinks(%{source: :internal})
      assert Enum.any?(sinks, &(&1.name == :ring_buffer))
    end
  end

  describe "query/2" do
    test "returns ring-buffer contents for source", %{src: src} do
      Bus.emit(
        Signal.new(%{
          kind: :event,
          source: src,
          name: :query_test
        })
      )

      TestSinks.settle(RingBufferSink)

      {:ok, results} =
        Observability.query(
          %{source: src, from_ts: 0},
          %{source: :ui}
        )

      assert is_list(results)
      assert Enum.any?(results, &(&1.name == :query_test))
    end

    test "missing :source param returns error" do
      assert {:error, %Flezha.Error{code: "observability.missing_source"}} =
               Observability.query(%{}, %{source: :ui})
    end
  end

  describe "enable_source / disable_source" do
    test "transitions status and is audited", %{src: src} do
      _ = RingBufferSink.query(:observability_service, 0)
      before_ts = System.monotonic_time(:microsecond)

      :ok =
        Observability.disable_source(src, %{source: :cli, request_id: "req-123"})

      TestSinks.settle(RingBufferSink)
      {:ok, %{status: :disabled}} = Registry.lookup_source(src)

      audits = RingBufferSink.query(:observability_service, before_ts)

      assert Enum.any?(audits, fn s ->
               s.name == :disable_source and s.metadata[:source] == src and
                 s.metadata[:caller_source] == :cli and s.metadata[:request_id] == "req-123"
             end)

      :ok = Observability.enable_source(src, %{source: :ui})
      {:ok, %{status: :active}} = Registry.lookup_source(src)
    end

    # Fails when: a known atom that names no source reaches the registry, which
    # answers a bare `:not_found`, or a tail subscribes to nothing and prints nothing.
    test "a word naming no registered source is refused with the sources that are", %{src: src} do
      for call <- [
            &Observability.enable_source/2,
            &Observability.disable_source/2,
            &Observability.tail/2
          ] do
        assert {:error, %Flezha.Error{code: "usage.unknown_word", message: message}} =
                 call.(:obs_svc_never_a_source, %{source: :internal})

        assert message =~ "unknown source 'obs_svc_never_a_source'"
        assert message =~ Atom.to_string(src)
      end
    end
  end

  describe "master disable" do
    test "every public function returns {:error, :observability_disabled} when flag is false" do
      prev = Application.get_env(:flezha, :observability, [])
      Application.put_env(:flezha, :observability, Keyword.put(prev, :enabled, false))

      try do
        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.list_sources(%{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.list_sinks(%{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.query(%{source: :s}, %{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.subscribe(:s, %{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.unsubscribe(:s, %{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.enable_source(:s, %{source: :cli})

        assert {:error, %Flezha.Error{code: "observability.disabled"}} =
                 Observability.disable_source(:s, %{source: :cli})
      after
        Application.put_env(:flezha, :observability, prev)
      end
    end
  end

  describe "unsubscribe/2" do
    test "undoes a subscription taken on the same surface", %{src: src} do
      claim_pubsub_sink()

      assert :ok = Observability.subscribe(src, %{source: :cli})
      assert :ok = Observability.unsubscribe(src, %{source: :cli})
    end

    test "refuses a caller with no context, as subscribe/2 does", %{src: src} do
      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.unsubscribe(src, %{})

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.subscribe(src, %{})
    end
  end

  describe "through the door" do
    test "a source written as text reaches tail/2 as the atom it names", %{src: src} do
      claim_pubsub_sink()

      assert {:ok, :ok} =
               Admin.run("Observability.tail", ~s(["#{src}"]), %{source: :cli}, :given)

      Bus.emit(Signal.new(%{kind: :event, source: src, name: :door_probe}))
      assert_receive {:flezha_signal, %Signal{source: ^src, name: :door_probe}}, 1_000

      assert {:ok, :ok} = Admin.run("Observability.unsubscribe", [src], %{source: :cli}, :given)
    end

    # Fails when: the named fields keep their text, so the read can never find
    # its source and always answers `missing_source`.
    test "a query written as JSON reads the ring buffer for its source", %{src: src} do
      Bus.emit(Signal.new(%{kind: :event, source: src, name: :door_query}))
      TestSinks.settle(RingBufferSink)

      assert {:ok, [%Signal{name: :door_query}]} =
               Admin.run(
                 "Observability.query",
                 ~s([{"source": "#{src}"}]),
                 %{source: :cli},
                 :given
               )
    end

    test "a source naming no atom is refused, and none is minted" do
      refuse = fn ->
        Admin.call(
          "Observability.tail",
          ~s(["obs_door_never_an_atom_7f3a"]),
          %{source: :cli},
          :given
        )
      end

      # The first refusal loads what refusing needs; a word it had minted would
      # be served the second time.
      assert {:error, %Flezha.Error{code: "admin.unknown_parameter"}} = refuse.()
      before = :erlang.system_info(:atom_count)

      assert {:error, %Flezha.Error{code: "admin.unknown_parameter", details: details}} =
               refuse.()

      assert details["parameter"] == "obs_door_never_an_atom_7f3a"
      assert :erlang.system_info(:atom_count) == before
    end
  end

  describe "the CLI renderers" do
    test "observe tail --json renders a metadata key of any shape", %{src: src} do
      # `stringify_keys/1` was a fourth copy of the term->string decision and
      # applied `to_string/1` to the KEY: a tuple key raised
      # Protocol.UndefinedError in the CLI, a list key was silently re-encoded
      # as the binary of its bytes, and a non-UTF-8 binary key made
      # `Jason.encode!/1` raise. All three now go through the shared rule.
      signal =
        Signal.new(%{
          kind: :event,
          source: src,
          name: :tail_probe,
          measurements: %{count: 1},
          metadata: %{
            {:a, :b} => 1,
            [1, 2, 3] => 2,
            <<0xFF, 0x00, 0xFE>> => 3,
            nil => 4,
            :ok_key => "v"
          }
        })

      json = capture_tail_json(src, signal)

      assert Map.keys(json["metadata"]) |> Enum.sort() ==
               Enum.sort(["{:a, :b}", "[1, 2, 3]", "<<255, 0, 254>>", "nil", "ok_key"])

      # The value half stays typed — JSON has numbers, and collapsing it onto
      # the text rule would stringify every one of them.
      assert json["metadata"]["{:a, :b}"] == 1
      assert json["measurements"]["count"] == 1
    end

    test "observe tail --json and the audit log answer one JSON value rule", %{src: src} do
      metadata = %{list: [1, 2, 3], nested: %{"a" => 1}, missing: nil, text: "t", n: 2}

      signal =
        Signal.new(%{
          kind: :event,
          source: src,
          name: :tail_value_probe,
          measurements: %{count: 1},
          metadata: metadata
        })

      json = capture_tail_json(src, signal)

      # JSON carries a null, an array and an object; rendering them as strings
      # is the text rule leaking into a format that does not need it.
      assert json["metadata"]["list"] == [1, 2, 3]
      assert json["metadata"]["nested"] == %{"a" => 1}
      assert json["metadata"]["missing"] == nil
      assert json["metadata"]["text"] == "t"
      assert json["metadata"]["n"] == 2

      # One rule, so the two JSON renderers of the same metadata agree.
      {:ok, audit} = Codec.decode(Codec.encode(signal))
      assert audit["metadata"] == json["metadata"]
    end
  end

  # Drives the real `render_tail_signal/2`: `do_tail_loop/3` reads the caller's
  # own mailbox, so seeding it before the call exercises the render path
  # without a live PubSub broadcast. The gate in front of the loop reads
  # `:pubsub_sink_enabled`, which `config/test.exs` sets false, so the test
  # owns that flag for its own duration rather than inheriting whatever the
  # previous test left.
  defp capture_tail_json(src, signal) do
    claim_pubsub_sink()
    send(self(), {:flezha_signal, signal})

    out =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Observability.tail", Jason.encode!([Atom.to_string(src)]),
          output: "json",
          duration_ms: 1
        )
      end)

    out |> String.split("\n", trim: true) |> List.first() |> Jason.decode!()
  end

  # `config/test.exs` sets `:pubsub_sink_enabled` false and starts no sink, and
  # every subscription gate reads both. The test owns the flag for its own
  # duration rather than inheriting whatever the previous one left.
  defp claim_pubsub_sink do
    prev = Application.get_env(:flezha, :observability, [])
    Application.put_env(:flezha, :observability, Keyword.put(prev, :pubsub_sink_enabled, true))
    on_exit(fn -> Application.put_env(:flezha, :observability, prev) end)

    if Process.whereis(PubSubSink) == nil do
      sink = :"obs_tail_pubsub_#{:erlang.unique_integer([:positive])}"
      {:ok, _pid} = Registry.register_sink(%{name: sink, module: PubSubSink, config: %{}})
      on_exit(fn -> Registry.unregister_sink(sink) end)
    end
  end
end
