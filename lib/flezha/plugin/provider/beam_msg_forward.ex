defmodule Flezha.Plugin.Provider.BeamMsgForward do
  @moduledoc """
  First-party `:on_input` / `:on_output` transform subscriber that forwards
  the payload's message to a remote BEAM process via
  `{:message_forward, message}` — or, with `include_metadata: true`, as
  `{:message_forward, message, metadata}` so a test sink can assert the
  wire stamps (e.g. the effectively-once dedup identity).

  Required opts:
    * `:recv_pid` (pid)

  Optional opts:
    * `:include_metadata` (boolean, default `false`)
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  # `:recv_pid` is total through `parse_pid/2`; no option needs the injected
  # refusal.
  @converted_opts []

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:recv_pid, value), do: ProviderBehaviour.parse_pid(value, :recv_pid)

  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: msg, metadata: metadata} = ctx, %{recv_pid: recv_pid} = opts) do
    payload =
      if Map.get(opts, :include_metadata, false) do
        {:message_forward, msg, metadata}
      else
        {:message_forward, msg}
      end

    case Process.send(recv_pid, payload, []) do
      :ok -> {:ok, ctx}
      _ -> {:error, "send_failed"}
    end
  end
end
