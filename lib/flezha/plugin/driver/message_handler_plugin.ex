defmodule Flezha.Plugin.Driver.MessageHandlerPlugin do
  @moduledoc """
  Plugin driver for per-processor typed request/reply message handlers.

  Unlike `Flezha.Plugin.Driver.IOBindingPlugin`, a `MessageHandlerPlugin`
  does not own a long-lived process — each matched request runs in a
  short-lived `Task` under `Flezha.Processor.MessageHandlerTaskSup`.
  `handler_key` must be unique per processor; a conflict at enable-time
  returns `{:error, {:handler_key_conflict, key}}` and the existing handler
  is left untouched.
  """

  alias Flezha.Plugin.PluginBehaviour
  alias Flezha.Service.ProcessorManager

  @behaviour PluginBehaviour

  @enforce_keys [:namespace, :processor_id, :handler_key, :provider]
  defstruct @enforce_keys ++ [opts: %{}]

  @type t() :: %__MODULE__{
          namespace: atom() | String.t(),
          processor_id: String.t(),
          handler_key: atom(),
          provider: module(),
          opts: map()
        }

  @impl true
  def recoverable?(%__MODULE__{}), do: true

  @impl true
  def enable(plugin_id, %__MODULE__{} = plugin) do
    case ProcessorManager.get_processor_pid(plugin.namespace, plugin.processor_id) do
      {:ok, processor_pid} ->
        case GenServer.call(processor_pid, {:enable_message_handler, plugin_id, plugin}) do
          :ok -> {:ok, {__MODULE__, {plugin.namespace, plugin.processor_id}}}
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  @impl true
  def disable(plugin_id, {namespace, processor_id}) do
    case ProcessorManager.get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        case GenServer.call(processor_pid, {:disable_message_handler, plugin_id}) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end
end
