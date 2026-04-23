defmodule Flezha.Processor.InitState.Providers.Sibling do
  @moduledoc """
  `:on_processor_init` subscriber that seeds a cold-starting processor from
  another running processor by sending a `{:flezha_request, handler_key,
  payload, self(), ref}` envelope to the source's Overwatch. The JSON-decoded
  reply is shallow-merged into the fold accumulator.

  ## Required opts
    * `:source_processor_id`
    * `:handler_key`

  ## Optional opts
    * `:source_namespace` (defaults to caller's namespace)
    * `:request_payload` (defaults to `<<>>`)
    * `:request_timeout_ms` (defaults to 4_000 so the inner receive times out
      before the fold's outer `Task.shutdown(:brutal_kill)` at 5_000)
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.Payloader

  @default_timeout_ms 4_000

  def on_processor_init(%Ctx.Init{} = ctx, opts) when is_map(opts) do
    with {:ok, source_processor_id} <- fetch_required(opts, :source_processor_id),
         {:ok, handler_key} <- fetch_required(opts, :handler_key),
         source_namespace <- Map.get(opts, :source_namespace, ctx.namespace),
         request_payload <- Map.get(opts, :request_payload, <<>>),
         timeout_ms <- Map.get(opts, :request_timeout_ms, @default_timeout_ms),
         {:ok, target_pid} <- resolve_pid(source_namespace, source_processor_id),
         {:ok, decoded_map} <- dispatch(target_pid, handler_key, request_payload, timeout_ms) do
      merged = Map.merge(ctx.accumulated, decoded_map)
      {:ok, %Ctx.Init{ctx | accumulated: merged}}
    end
  end

  defp fetch_required(opts, key) do
    case Map.fetch(opts, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_opt, key}}
    end
  end

  defp resolve_pid(namespace, processor_id) do
    case ProcessorManager.get_processor_pid(namespace, processor_id) do
      {:ok, pid} -> {:ok, pid}
      _ -> {:error, :source_processor_not_found}
    end
  end

  defp dispatch(target_pid, handler_key, payload, timeout_ms) do
    ref = make_ref()
    send(target_pid, {:flezha_request, handler_key, payload, self(), ref})

    receive do
      {:flezha_response, ^ref, {:ok, bytes}} -> decode_reply(bytes)
      {:flezha_response, ^ref, {:error, reason}} -> {:error, reason}
    after
      timeout_ms -> {:error, :timeout}
    end
  end

  defp decode_reply(bytes) when is_binary(bytes) do
    try do
      case Payloader.json_to_map(bytes) do
        %{} = map -> {:ok, map}
        _other -> {:error, {:invalid_reply_payload, :not_a_map}}
      end
    rescue
      e -> {:error, {:invalid_reply_payload, Exception.message(e)}}
    end
  end
end
