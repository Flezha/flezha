defmodule FlezhaTest.Processor.SiblingTest do
  @moduledoc """
  Unit tests for `Flezha.Processor.InitState.Providers.Sibling` under
  the extension-system contract (`on_processor_init/2`).
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Processor.InitState.Providers.Sibling
  alias Flezha.Service.ProcessorManager

  @base_ctx %Ctx.Init{
    processor_id: "consumer",
    namespace: "default",
    factory_id: "store_input",
    accumulated: %{}
  }

  defp spawn_fake_target(reply_fn) do
    spawn(fn ->
      receive do
        {:flezha_request, _key, payload, reply_to, ref} ->
          result = reply_fn.(payload)
          send(reply_to, {:flezha_response, ref, result})
      after
        2_000 -> :ok
      end
    end)
  end

  defp stub_pid(namespace, processor_id, pid_or_error) do
    expect(ProcessorManager, :get_processor_pid, fn
      ^namespace, ^processor_id ->
        case pid_or_error do
          {:ok, _} = ok -> ok
          {:error, _} = err -> err
        end
    end)
  end

  test "missing source processor → {:error, :source_processor_not_found}" do
    stub_pid("default", "missing", {:error, "processor not found"})

    opts = %{source_processor_id: "missing", handler_key: :anything}

    assert {:error, :source_processor_not_found} = Sibling.on_processor_init(@base_ctx, opts)
  end

  test "handler times out → {:error, :timeout}" do
    silent = spawn(fn -> Process.sleep(1_000) end)
    stub_pid("default", "src", {:ok, silent})

    opts = %{source_processor_id: "src", handler_key: :ignored, request_timeout_ms: 20}

    assert {:error, :timeout} = Sibling.on_processor_init(@base_ctx, opts)
  end

  test "reply is non-object JSON → {:error, {:invalid_reply_payload, :not_a_map}}" do
    fake = spawn_fake_target(fn _payload -> {:ok, Jason.encode!([1, 2, 3])} end)
    stub_pid("default", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: :fetch}

    assert {:error, {:invalid_reply_payload, :not_a_map}} =
             Sibling.on_processor_init(@base_ctx, opts)
  end

  test "reply is invalid JSON → {:error, {:invalid_reply_payload, _}}" do
    fake = spawn_fake_target(fn _payload -> {:ok, "not-json"} end)
    stub_pid("default", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: :fetch}

    assert {:error, {:invalid_reply_payload, _reason}} =
             Sibling.on_processor_init(@base_ctx, opts)
  end

  test "successful JSON object reply → {:ok, merged ctx}" do
    fake = spawn_fake_target(fn _payload -> {:ok, Jason.encode!(%{"a" => 1, "b" => [2]})} end)
    stub_pid("default", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: :fetch}

    assert {:ok, %Ctx.Init{accumulated: %{"a" => 1, "b" => [2]}}} =
             Sibling.on_processor_init(@base_ctx, opts)
  end

  test "handler returns {:error, reason} → propagated unchanged" do
    fake = spawn_fake_target(fn _payload -> {:error, :snapshot_unavailable} end)
    stub_pid("default", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: :fetch}

    assert {:error, :snapshot_unavailable} = Sibling.on_processor_init(@base_ctx, opts)
  end

  test ":source_namespace overrides the default context.namespace" do
    fake = spawn_fake_target(fn _ -> {:ok, "{}"} end)
    stub_pid("other-ns", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: :fetch, source_namespace: "other-ns"}

    assert {:ok, %Ctx.Init{accumulated: %{}}} = Sibling.on_processor_init(@base_ctx, opts)
  end

  test "missing :handler_key → {:error, {:missing_opt, :handler_key}}" do
    opts = %{source_processor_id: "src"}

    assert {:error, {:missing_opt, :handler_key}} =
             Sibling.on_processor_init(@base_ctx, opts)
  end

  test "missing :source_processor_id → {:error, {:missing_opt, :source_processor_id}}" do
    opts = %{handler_key: :fetch}

    assert {:error, {:missing_opt, :source_processor_id}} =
             Sibling.on_processor_init(@base_ctx, opts)
  end

  # Regression guard — these three tests catch the failure modes that
  # bit flezha_market's init-state wiring during fix-processor-state-
  # rehydration: (1) CLI-supplied opts arrive with string keys/values, so
  # handler_key is a string but the `:on_message` subscription lookup
  # keys by atom; (2) request_payload arrives as a map (JSON-decoded from
  # the --plugins flag) even though the transport expects bytes; (3) the
  # source processor may still be booting during a DETS replay iteration
  # in which the consumer is scheduled before the provider.

  test "string handler_key is atomized so `:on_message` dispatch matches" do
    fake =
      spawn(fn ->
        receive do
          {:flezha_request, key, _payload, reply_to, ref} ->
            # Assert Sibling atomized `"fetch_init_state"` → `:fetch_init_state`.
            assert is_atom(key)
            assert key == :fetch_init_state
            send(reply_to, {:flezha_response, ref, {:ok, "{}"}})
        after
          2_000 -> :ok
        end
      end)

    stub_pid("default", "src", {:ok, fake})

    opts = %{source_processor_id: "src", handler_key: "fetch_init_state"}

    assert {:ok, %Ctx.Init{accumulated: %{}}} = Sibling.on_processor_init(@base_ctx, opts)
  end

  test "map request_payload is JSON-encoded before dispatch" do
    fake =
      spawn(fn ->
        receive do
          {:flezha_request, _key, payload, reply_to, ref} ->
            # The transport expects binary; Sibling must Jason.encode! the
            # map from opts before sending.
            assert is_binary(payload)
            assert Jason.decode!(payload) == %{"symbol" => "BTC/DEMO1", "ohlc_window" => 1000}
            send(reply_to, {:flezha_response, ref, {:ok, "{}"}})
        after
          2_000 -> :ok
        end
      end)

    stub_pid("default", "src", {:ok, fake})

    opts = %{
      source_processor_id: "src",
      handler_key: :fetch,
      request_payload: %{"symbol" => "BTC/DEMO1", "ohlc_window" => 1000}
    }

    assert {:ok, %Ctx.Init{accumulated: %{}}} = Sibling.on_processor_init(@base_ctx, opts)
  end

  # NOTE: Sibling's resolve_pid retry behaviour is verified at the live
  # agent-log level ("source_processor_not_found" → "[init-state-sibling]
  # delivered" within the same request_timeout_ms window during DETS
  # replay) rather than here, because a Mimic stub doesn't easily model
  # "first call errors, later calls succeed" at the ~100 ms poll cadence
  # without racing against the monotonic deadline.
end
