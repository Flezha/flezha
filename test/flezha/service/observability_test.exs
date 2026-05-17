defmodule Flezha.Service.ObservabilityTest do
  use ExUnit.Case, async: false

  alias Flezha.Service.{CliManager, Observability}
  alias Flezha.Observability.{Registry, RingBufferSink}

  setup do
    src = :"obs_svc_test_src_#{:erlang.unique_integer([:positive])}"
    :ok = Registry.register_source(%{name: src, kinds: [:event], flavour: :declarative})
    on_exit(fn -> Registry.unregister_source(src) end)
    {:ok, src: src}
  end

  describe "caller_context validation" do
    test "rejects missing :source field" do
      assert {:error, :invalid_caller_context} = Observability.list_sources(%{})
    end

    test "rejects invalid :source value" do
      assert {:error, :invalid_caller_context} =
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
      Flezha.Observability.Bus.emit(
        Flezha.Observability.Signal.new(%{
          kind: :event,
          source: src,
          name: :query_test
        })
      )

      Process.sleep(50)

      {:ok, results} =
        Observability.query(
          %{source: src, from_ts: 0},
          %{source: :ui}
        )

      assert is_list(results)
      assert Enum.any?(results, &(&1.name == :query_test))
    end

    test "missing :source param returns error" do
      assert {:error, :missing_source} = Observability.query(%{}, %{source: :ui})
    end
  end

  describe "enable_source / disable_source" do
    test "transitions status and is audited", %{src: src} do
      _ = RingBufferSink.query(:observability_service, 0)
      before_ts = System.monotonic_time(:microsecond)

      :ok =
        Observability.disable_source(src, %{source: :cli, request_id: "req-123"})

      Process.sleep(50)
      {:ok, %{status: :disabled}} = Registry.lookup_source(src)

      audits = RingBufferSink.query(:observability_service, before_ts)

      assert Enum.any?(audits, fn s ->
               s.name == :disable_source and s.metadata[:source] == src and
                 s.metadata[:caller_source] == :cli and s.metadata[:request_id] == "req-123"
             end)

      :ok = Observability.enable_source(src, %{source: :ui})
      Process.sleep(20)
      {:ok, %{status: :active}} = Registry.lookup_source(src)
    end
  end

  describe "master disable" do
    test "every public function returns {:error, :observability_disabled} when flag is false" do
      prev = Application.get_env(:flezha, :observability, [])
      Application.put_env(:flezha, :observability, Keyword.put(prev, :enabled, false))

      try do
        assert {:error, :observability_disabled} =
                 Observability.list_sources(%{source: :cli})

        assert {:error, :observability_disabled} =
                 Observability.list_sinks(%{source: :cli})

        assert {:error, :observability_disabled} =
                 Observability.query(%{source: :s}, %{source: :cli})

        assert {:error, :observability_disabled} =
                 Observability.subscribe(:s, %{source: :cli})

        assert {:error, :observability_disabled} =
                 Observability.enable_source(:s, %{source: :cli})

        assert {:error, :observability_disabled} =
                 Observability.disable_source(:s, %{source: :cli})
      after
        Application.put_env(:flezha, :observability, prev)
      end
    end
  end

  describe "CliManager wrappers" do
    test "list_sources via CliManager calls service API with :cli context", %{src: src} do
      {:ok, sources} = CliManager.observability_list_sources()
      assert Enum.any?(sources, &(&1.name == src))
    end

    test "enable_source via CliManager builds :cli context", %{src: src} do
      assert :ok = CliManager.observability_disable_source(src)
      Process.sleep(20)
      {:ok, %{status: :disabled}} = Registry.lookup_source(src)
      assert :ok = CliManager.observability_enable_source(src)
    end
  end
end
