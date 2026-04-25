defmodule Flezha.Processor.InitState.Providers.Sibling do
  @moduledoc """
  **DEPRECATED.** Use `Flezha.Cluster.Snapshot.Coordinator.restore/2` (or
  `flz cluster restore`) for restart-survival init-state. The Sibling
  provider is retained for backward compatibility and is targeted for
  removal in the next major version.

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

  @deprecated "Use Flezha.Cluster.Snapshot.Coordinator.restore/2 (or 'flz cluster restore') for restart-survival init-state. The Sibling provider is retained for backward compatibility and is targeted for removal in the next major version."

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.Payloader

  require Logger

  @default_timeout_ms 4_000

  def on_processor_init(%Ctx.Init{} = ctx, opts) when is_map(opts) do
    result =
      with {:ok, source_processor_id} <- fetch_required(opts, :source_processor_id),
           {:ok, handler_key_raw} <- fetch_required(opts, :handler_key),
           handler_key <- normalize_handler_key(handler_key_raw),
           source_namespace <- Map.get(opts, :source_namespace, ctx.namespace),
           request_payload <- coerce_request_payload(Map.get(opts, :request_payload, <<>>)),
           timeout_ms <- Map.get(opts, :request_timeout_ms, @default_timeout_ms),
           # Split the budget roughly 50/50 between waiting for the source
           # processor to come online and dispatching the RPC. During a
           # recovery replay the consumer processor (btc_demo) is often
           # scheduled before the provider (market-init-state); the poll
           # loop gives the provider a moment to arrive.
           resolve_budget_ms <- div(timeout_ms, 2),
           dispatch_budget_ms <- timeout_ms - resolve_budget_ms,
           {:ok, target_pid} <-
             resolve_pid(source_namespace, source_processor_id, resolve_budget_ms),
           {:ok, decoded_map} <-
             dispatch(target_pid, handler_key, request_payload, dispatch_budget_ms) do
        merged = Map.merge(ctx.accumulated, decoded_map)
        {:ok, %Ctx.Init{ctx | accumulated: merged}}
      end

    case result do
      {:ok, %Ctx.Init{}} = ok ->
        Logger.info(
          "[init-state-sibling] delivered processor=#{ctx.processor_id} " <>
            "keys=#{inspect(Map.keys(Map.get(elem(ok, 1), :accumulated, %{})))}"
        )

        ok

      {:error, reason} = err ->
        Logger.warning(
          "[init-state-sibling] failed processor=#{ctx.processor_id} reason=#{inspect(reason)}"
        )

        err
    end
  end

  # Subscription keys are stored as atoms (see
  # `Flezha.Plugin.Extension.Table.validate_kind_specific(:request, ...)`), but
  # opts come in from JSON with the handler key as a string. Atomize so the
  # `Ctx.Msg{key: key}` at dispatch time matches the `request_subs` table.
  defp normalize_handler_key(v) when is_atom(v), do: v
  defp normalize_handler_key(v) when is_binary(v), do: String.to_atom(v)

  # request_payload may arrive as a map (when the plugin JSON embeds a
  # structured payload) or a binary (when the caller pre-serialised). The
  # transport forwards binaries verbatim, so encode maps to JSON bytes here.
  defp coerce_request_payload(payload) when is_binary(payload), do: payload
  defp coerce_request_payload(payload) when is_map(payload), do: Jason.encode!(payload)

  defp fetch_required(opts, key) do
    case Map.fetch(opts, key) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_opt, key}}
    end
  end

  # During recovery the source processor may still be booting on a sibling
  # replay iteration. Poll up to `budget_ms` before giving up so the
  # init-state plugin isn't sensitive to the DETS replay order.
  @resolve_poll_ms 100

  defp resolve_pid(namespace, processor_id, budget_ms) when is_integer(budget_ms) do
    deadline = System.monotonic_time(:millisecond) + max(budget_ms, 0)
    do_resolve_pid(namespace, processor_id, deadline)
  end

  defp do_resolve_pid(namespace, processor_id, deadline_ms) do
    case ProcessorManager.get_processor_pid(namespace, processor_id) do
      {:ok, pid} ->
        {:ok, pid}

      _ ->
        now = System.monotonic_time(:millisecond)

        if now < deadline_ms do
          Process.sleep(@resolve_poll_ms)
          do_resolve_pid(namespace, processor_id, deadline_ms)
        else
          {:error, :source_processor_not_found}
        end
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
