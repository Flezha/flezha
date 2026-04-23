defmodule Flezha.Plugin.Provider.BeamMsgForward do
  @moduledoc """
  First-party `:on_input` / `:on_output` transform subscriber that forwards
  the payload's message to a remote BEAM process via
  `{:message_forward, message}`.

  Required opts:
    * `:recv_pid` (pid)
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.Parser

  use ProviderBehaviour

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:recv_pid, value) when is_binary(value) do
    Parser.string_to_pid(value)
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: msg} = ctx, %{recv_pid: recv_pid}) do
    case Process.send(recv_pid, {:message_forward, msg}, []) do
      :ok -> {:ok, ctx}
      _ -> {:error, "send_failed"}
    end
  end
end
