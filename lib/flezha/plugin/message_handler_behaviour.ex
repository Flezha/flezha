defmodule Flezha.Plugin.MessageHandlerBehaviour do
  @moduledoc """
  Behaviour for providers that answer a processor's typed message-handler
  requests.

  A provider is invoked once per incoming `{:flezha_request, handler_key,
  payload, reply_to, ref}` envelope matched by Overwatch. The invocation runs
  in a short-lived `Task` under `Flezha.Processor.MessageHandlerTaskSup` —
  not on the Overwatch process — so a slow handler cannot stall IO or other
  lifecycle messages for the processor.

  The `context` map carries:

    * `:processor_id` (`String.t()`) — target processor's id
    * `:namespace` (`atom() | String.t()`) — target processor's namespace
    * `:compute_engine_id` (`String.t()`)
    * `:handler_key` (`atom()`) — the matched handler key
    * `:from` (`pid()`) — the caller's pid (use to gate requests)
    * `:opts` (`map()`) — the plugin's configured `opts`

  Return `{:ok, binary()}` to reply with opaque bytes, or `{:error, term()}`
  to surface a structured error to the caller.
  """

  @type context :: %{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t(),
          handler_key: atom(),
          from: pid(),
          opts: map()
        }

  @callback handle(payload :: binary(), context :: context()) ::
              {:ok, binary()} | {:error, term()}
end
