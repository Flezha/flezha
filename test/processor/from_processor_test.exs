defmodule FlezhaTest.Processor.FromProcessorTest do
  @moduledoc """
  Unit tests for `Flezha.Processor.InitState.Providers.FromProcessor`. Each
  test fakes the target Overwatch by `send`-ing a canned response from a
  spawned process, then stubs `ProcessorManager.get_processor_pid/2` to
  return that fake pid.
  """

  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Processor.InitState.Providers.FromProcessor
  alias Flezha.Service.ProcessorManager

  @base_context %{
    processor_id: "consumer",
    namespace: "default",
    factory_id: "store_input",
    accumulated: %{}
  }

  # A fake Overwatch that replies to the first `{:flezha_request, ...}` it
  # sees with a caller-supplied result, then exits normally.
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

  test "10.7 missing source processor → {:error, :source_processor_not_found}" do
    stub_pid("default", "missing", {:error, "processor not found"})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "missing",
        handler_key: :anything
      })

    assert {:error, :source_processor_not_found} = FromProcessor.resolve(context)
  end

  test "10.8 handler times out → {:error, :timeout}" do
    # Fake target that never replies.
    silent = spawn(fn -> Process.sleep(1_000) end)
    stub_pid("default", "src", {:ok, silent})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :ignored,
        request_timeout_ms: 20
      })

    assert {:error, :timeout} = FromProcessor.resolve(context)
  end

  test "10.9 reply is non-object JSON → {:error, {:invalid_reply_payload, :not_a_map}}" do
    fake =
      spawn_fake_target(fn _payload ->
        {:ok, Jason.encode!([1, 2, 3])}
      end)

    stub_pid("default", "src", {:ok, fake})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :fetch
      })

    assert {:error, {:invalid_reply_payload, :not_a_map}} = FromProcessor.resolve(context)
  end

  test "10.10 reply is invalid JSON → {:error, {:invalid_reply_payload, _}}" do
    fake =
      spawn_fake_target(fn _payload ->
        {:ok, "not-json"}
      end)

    stub_pid("default", "src", {:ok, fake})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :fetch
      })

    assert {:error, {:invalid_reply_payload, _reason}} = FromProcessor.resolve(context)
  end

  test "successful JSON object reply → {:ok, map}" do
    fake =
      spawn_fake_target(fn _payload ->
        {:ok, Jason.encode!(%{"a" => 1, "b" => [2]})}
      end)

    stub_pid("default", "src", {:ok, fake})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :fetch
      })

    assert {:ok, %{"a" => 1, "b" => [2]}} = FromProcessor.resolve(context)
  end

  test "handler returns {:error, reason} → propagated unchanged" do
    fake = spawn_fake_target(fn _payload -> {:error, :snapshot_unavailable} end)
    stub_pid("default", "src", {:ok, fake})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :fetch
      })

    assert {:error, :snapshot_unavailable} = FromProcessor.resolve(context)
  end

  test ":source_namespace overrides the default context.namespace" do
    fake = spawn_fake_target(fn _ -> {:ok, "{}"} end)
    stub_pid("other-ns", "src", {:ok, fake})

    context =
      Map.put(@base_context, :opts, %{
        source_processor_id: "src",
        handler_key: :fetch,
        source_namespace: "other-ns"
      })

    assert {:ok, %{}} = FromProcessor.resolve(context)
  end

  test "missing :handler_key → {:error, {:missing_opt, :handler_key}}" do
    context =
      Map.put(@base_context, :opts, %{source_processor_id: "src"})

    assert {:error, {:missing_opt, :handler_key}} = FromProcessor.resolve(context)
  end

  test "missing :source_processor_id → {:error, {:missing_opt, :source_processor_id}}" do
    context = Map.put(@base_context, :opts, %{handler_key: :fetch})

    assert {:error, {:missing_opt, :source_processor_id}} = FromProcessor.resolve(context)
  end
end
