defmodule Flezha.Service.ObservabilityTest do
  # async: false: writes Application env; the observability registry.
  use ExUnit.Case, async: false

  import FlezhaTest.Case, only: [refute_minted: 2]

  alias Flezha.Observability.AuditLog.Codec
  alias Flezha.Observability.Bus
  alias Flezha.Observability.{PubSubSink, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Service.{Admin, Cli, Observability}
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper}

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

  describe "list_sources recent activity" do
    test "counts each signal name the window holds and ages the newest", %{src: src} do
      window_ms = Flezha.Config.get(:observability_ring_buffer_retention_ms)
      old_ts = System.monotonic_time(:microsecond) - (window_ms + 1_000) * 1_000

      Bus.emit(Signal.new(%{kind: :event, source: src, name: :old, ts: old_ts}))
      for name <- [:a, :a, :b], do: Bus.emit(Signal.new(%{kind: :event, source: src, name: name}))
      TestSinks.settle(RingBufferSink)

      %{recent: recent} = listed(src, %{source: :internal})

      assert recent.window_ms == window_ms
      assert %{a: %{count: 2, last_age_ms: age}, b: %{count: 1}} = recent.signals
      refute Map.has_key?(recent.signals, :old)
      assert age in 0..5_000
    end

    test "a source that said nothing in the window has no signals" do
      quiet = :"obs_svc_quiet_#{:erlang.unique_integer([:positive])}"
      :ok = Registry.register_source(%{name: quiet, kinds: [:event], flavour: :declarative})
      on_exit(fn -> Registry.unregister_source(quiet) end)
      TestSinks.settle(RingBufferSink)

      assert %{recent: %{signals: signals}} = listed(quiet, %{source: :internal})
      assert signals == %{}
    end

    test "counts only the signals a namespace-bound caller may read", %{src: src} do
      AuthTestHelper.setup_auth_stores(prefix: "flz_obs_recent", claim: true)

      for {name, metadata} <- [own: %{namespace: "team-a"}, other: %{namespace: "b"}, bare: %{}],
          do: Bus.emit(Signal.new(%{kind: :event, source: src, name: name, metadata: metadata}))

      TestSinks.settle(RingBufferSink)

      context = %{source: :ui, principal: "viewer@team-a", namespace: "team-a", role: :viewer}
      %{recent: %{signals: signals}} = listed(src, context)

      assert signals |> Map.keys() |> Enum.sort() == [:bare, :own]
    end

    test "is nil while the ring buffer is not running", %{src: src} do
      :ok = Registry.unregister_sink(:ring_buffer)

      on_exit(fn ->
        {:ok, _pid} =
          Registry.register_sink(%{name: :ring_buffer, module: RingBufferSink, config: %{}})
      end)

      assert %{recent: nil} = listed(src, %{source: :internal})
    end
  end

  defp listed(name, context) do
    {:ok, sources} = Observability.list_sources(context)
    Enum.find(sources, &(&1.name == name))
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

    test "a namespace-bound caller's limit counts only what it may read", %{src: src} do
      AuthTestHelper.setup_auth_stores(prefix: "flz_obs_query_limit", claim: true)

      Bus.emit(
        Signal.new(%{kind: :event, source: src, name: :own, metadata: %{namespace: "team-a"}})
      )

      for _ <- 1..3,
          do:
            Bus.emit(
              Signal.new(%{kind: :event, source: src, name: :other, metadata: %{namespace: "b"}})
            )

      TestSinks.settle(RingBufferSink)

      context = %{source: :ui, principal: "viewer@team-a", namespace: "team-a", role: :viewer}
      assert {:ok, [%Signal{name: :own}]} = Observability.query(%{source: src, limit: 2}, context)
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

  # Fails when: the switch is judged in the caller's own namespace, so a
  # namespace's ops role flips the one status row the whole controller reads.
  describe "switching a source is a controller-wide act" do
    @shop_ops %{source: :cli, principal: "op@shop", role: :ops, namespace: "shop"}
    @system_ops %{source: :cli, principal: "op@_system", role: :ops, namespace: "_system"}

    setup do
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_obs_switch",
        claim: true,
        register_source: true
      )

      on_exit(fn -> Registry.enable_source(:platform_authz) end)
      :ok
    end

    test "an ops caller of one namespace cannot silence the audit trail" do
      for switch <- [&Observability.disable_source/2, &Observability.enable_source/2] do
        assert {:error, %Flezha.Error{code: "authz.namespace_mismatch"}} =
                 switch.(:platform_authz, @shop_ops)
      end

      assert {:ok, %{status: :active}} = Registry.lookup_source(:platform_authz)
    end

    test "an ops caller bound to _system still switches a source", %{src: src} do
      assert :ok = Observability.disable_source(src, @system_ops)
      assert {:ok, %{status: :disabled}} = Registry.lookup_source(src)
      assert :ok = Observability.enable_source(src, @system_ops)
      assert {:ok, %{status: :active}} = Registry.lookup_source(src)
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
    test "undoes a tail taken on the same surface", %{src: src} do
      claim_pubsub_sink()

      assert :ok = Observability.tail(src, %{source: :cli})
      assert :ok = Observability.unsubscribe(src, %{source: :cli})
    end

    test "refuses a caller with no context, as tail/2 does", %{src: src} do
      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.unsubscribe(src, %{})

      assert {:error, %Flezha.Error{code: "authz.invalid_caller_context"}} =
               Observability.tail(src, %{})
    end
  end

  describe "through the door" do
    test "a source written as text reaches tail/2 as the atom it names", %{src: src} do
      claim_pubsub_sink()

      assert {:ok, :ok} =
               Admin.run("Observability.tail", ~s(["#{src}"]), %{source: :cli}, :given)

      Bus.emit(Signal.new(%{kind: :event, source: src, name: :door_probe}))
      assert_receive {:flezha_signal, %Signal{source: ^src, name: :door_probe}}, 1_000

      assert {:ok, :ok} =
               Admin.run("Observability.unsubscribe", ~s(["#{src}"]), %{source: :cli}, :given)
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

    # Fails when: a mistyped source that happens to name no atom is refused as
    # `control.unknown_parameter`, without the sources a user could have meant,
    # while the same mistake on a word that is an atom names them.
    test "a mistyped source answers the source list whether or not the word is an atom",
         %{src: src} do
      verbs = ~w(unsubscribe tail enable_source disable_source)

      for verb <- verbs, word <- ["obs_door_never_an_atom_7f3a", "ok"] do
        answer =
          refute_minted(["obs_door_never_an_atom_7f3a"], fn ->
            Admin.call("Observability." <> verb, Jason.encode!([word]), %{source: :cli}, :given)
          end)

        assert {:error, %Flezha.Error{code: "usage.unknown_word", message: message}} = answer
        assert message =~ "unknown source '#{word}'"
        assert message =~ Atom.to_string(src)
      end
    end
  end

  # A consumer on another node reaches the controller through an `:erpc`
  # worker, whose `self()` dies when the call returns: the tail has to run in
  # that worker and hand each signal to a pid the consumer names.
  describe "a tail forwarded to a pid" do
    test "hands each signal on as {:flezha_emit, signal} and ends when the pid exits",
         %{src: src} do
      claim_pubsub_sink()
      {sink, tail} = forwarded_tail(src, %{source: :ui})

      Bus.emit(Signal.new(%{kind: :event, source: src, name: :forwarded}))
      assert_receive {:forwarded, %Signal{source: ^src, name: :forwarded}}, 2_000

      Process.exit(sink, :kill)
      assert Task.await(tail, 2_000) == {:ok, :caller_down}
      assert subscribers(src) == []
    end

    test "ends when the context's window runs out", %{src: src} do
      claim_pubsub_sink()

      assert {:ok, :window_closed} =
               :erpc.call(node(), Observability, :tail, [
                 src,
                 %{source: :ui, emit: self(), window_ms: 50}
               ])

      assert subscribers(src) == []
    end

    test "drops a signal of a namespace the caller may not read", %{src: src} do
      claim_pubsub_sink()
      AuthTestHelper.setup_auth_stores(prefix: "flz_obs_tail", claim: true)
      context = %{source: :ui, principal: "viewer@team-a", namespace: "team-a", role: :viewer}
      {sink, tail} = forwarded_tail(src, context)

      Bus.emit(
        Signal.new(%{kind: :event, source: src, name: :other, metadata: %{namespace: "b"}})
      )

      Bus.emit(
        Signal.new(%{kind: :event, source: src, name: :own, metadata: %{namespace: "team-a"}})
      )

      assert_receive {:forwarded, %Signal{name: :own}}, 2_000
      refute_received {:forwarded, %Signal{name: :other}}

      Process.exit(sink, :kill)
      assert {:ok, :caller_down} = Task.await(tail, 2_000)
    end
  end

  describe "the CLI renderers" do
    test "observe sources prints each source's title and description, and --json its keys" do
      described = :"obs_svc_described_#{:erlang.unique_integer([:positive])}"

      signals =
        Flezha.Observability.Source.signals(
          said: {:event, label: "Said", outcome: :failure, meaning: "It said something."}
        )

      :ok =
        Registry.register_source(%{
          name: described,
          kinds: [:event],
          flavour: :declarative,
          area: :relays,
          title: "Probe",
          description: "A described probe.",
          signals: signals
        })

      on_exit(fn -> Registry.unregister_source(described) end)

      text =
        ExUnit.CaptureIO.capture_io(fn -> Cli.call("Observability.list_sources", "[]", []) end)

      assert text =~ "name\tstatus\tkinds\tbudget\ttitle\tdescription"
      assert text =~ "#{described}\tactive\tevent\t1000\tProbe\tA described probe."

      json =
        ExUnit.CaptureIO.capture_io(fn ->
          Cli.call("Observability.list_sources", "[]", output: "json")
        end)

      entry = json |> Jason.decode!() |> Enum.find(&(&1["name"] == to_string(described)))
      assert entry["title"] == "Probe"
      assert entry["description"] == "A described probe."
      assert entry["area"] == "relays"

      assert [%{"name" => "said", "kind" => "event", "label" => "Said", "outcome" => "failure"}] =
               entry["signals"]

      assert %{"window_ms" => _, "signals" => %{}} = entry["recent"]
    end

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

  # A tail held by an `:erpc` worker on behalf of a sink process that forwards
  # what it is handed to the test; answers once the worker has subscribed.
  defp forwarded_tail(src, context) do
    test = self()

    sink = spawn(fn -> forward_to(test) end)

    tail =
      Task.async(fn ->
        :erpc.call(node(), Observability, :tail, [src, Map.put(context, :emit, sink)])
      end)

    assert NodeTestHelper.wait_until(fn -> subscribers(src) != [] end, 2_000)
    {sink, tail}
  end

  defp forward_to(test) do
    receive do
      {:flezha_emit, signal} -> send(test, {:forwarded, signal})
    end

    forward_to(test)
  end

  defp subscribers(src),
    do: Elixir.Registry.lookup(Flezha.Observability.PubSub, PubSubSink.topic_for(src))

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
