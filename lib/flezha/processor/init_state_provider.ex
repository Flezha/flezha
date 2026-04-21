defmodule Flezha.Processor.InitStateProvider do
  @moduledoc """
  Behaviour for modules that resolve a processor's initial state at startup.

  A provider is invoked once per processor cold-start via
  `Flezha.Processor.InitStateResolver.resolve/2`. Providers are one-shot and
  synchronous; they never run on migration and do not receive further
  lifecycle callbacks.

  The `context` argument is a map with these keys:

    * `:processor_id` (`String.t()`)
    * `:namespace` (`atom()`)
    * `:factory_id` (`String.t()`)
    * `:accumulated` (`map()`) — merged result of prior plugins in the chain
    * `:opts` (`map()`) — the plugin's own `%InitStatePlugin{}.opts`

  Return `{:ok, map}` to shallow-merge `map` into the accumulator, or
  `{:error, reason}` to trigger the plugin's `on_error` policy.
  """

  @type context :: %{
          processor_id: String.t(),
          namespace: atom(),
          factory_id: String.t(),
          accumulated: map(),
          opts: map()
        }

  @callback resolve(context :: context()) :: {:ok, map()} | {:error, term()}
end
