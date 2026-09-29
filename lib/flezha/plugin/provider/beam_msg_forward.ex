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

  use ProviderBehaviour,
    options: [
      recv_pid: [type: :pid, required: true, doc: "The BEAM process the payload is sent to"],
      include_metadata: [
        type: :bool,
        default: false,
        doc:
          "Send `{:message_forward, message, metadata}` instead of `{:message_forward, message}`"
      ]
    ]

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: forward(ctx, opts)

  defp forward(%Ctx.IO{message: msg, metadata: metadata} = ctx, %{recv_pid: recv_pid} = opts) do
    payload =
      if Map.fetch!(opts, :include_metadata) do
        {:message_forward, msg, metadata}
      else
        {:message_forward, msg}
      end

    case Process.send(recv_pid, payload, []) do
      :ok -> {:ok, ctx}
      _ -> {:error, :send_failed}
    end
  end
end
