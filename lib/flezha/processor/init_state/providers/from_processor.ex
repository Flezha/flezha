defmodule Flezha.Processor.InitState.Providers.FromProcessor do
  @moduledoc """
  Init-state provider that seeds a cold-starting processor from another
  running processor.

  Sends a typed request envelope to the source processor's Overwatch using
  the `processor-message-handlers` routing mechanism and shallow-merges the
  JSON-decoded reply into the init-state chain.

  ## Required opts

    * `:source_processor_id` (`String.t()`) — target processor's id
    * `:handler_key` (`atom()`) — the handler key to invoke on the target

  ## Optional opts

    * `:source_namespace` — defaults to `context.namespace`
    * `:request_payload` (`binary()`) — defaults to `<<>>`
    * `:request_timeout_ms` (`pos_integer()`) — defaults to `4_000`, kept
      strictly below the enclosing `%InitStatePlugin{}.timeout_ms` (5_000)
      so the inner `receive` times out before the outer `Task.shutdown`
      fires.
  """

  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.Payloader

  @behaviour Flezha.Processor.InitStateProvider

  @default_timeout_ms 4_000

  @impl true
  def resolve(%{opts: opts} = context) when is_map(opts) do
    with {:ok, source_processor_id} <- fetch_required(opts, :source_processor_id),
         {:ok, handler_key} <- fetch_required(opts, :handler_key),
         source_namespace <- Map.get(opts, :source_namespace, context.namespace),
         request_payload <- Map.get(opts, :request_payload, <<>>),
         timeout_ms <- Map.get(opts, :request_timeout_ms, @default_timeout_ms),
         {:ok, target_pid} <- resolve_pid(source_namespace, source_processor_id) do
      dispatch(target_pid, handler_key, request_payload, timeout_ms)
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
      {:flezha_response, ^ref, {:ok, bytes}} ->
        decode_reply(bytes)

      {:flezha_response, ^ref, {:error, reason}} ->
        {:error, reason}
    after
      timeout_ms -> {:error, :timeout}
    end
  end

  # `Payloader.json_to_map/1` raises on invalid JSON — wrap in try/rescue
  # so callers always see `{:error, {:invalid_reply_payload, _}}` rather
  # than a propagated exception. Non-object JSON values decode cleanly
  # under `Payloader.json_to_map/1` and are rejected here.
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
