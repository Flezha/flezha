defmodule Flezha.Mcp.Stdio do
  @moduledoc """
  The answers behind `flz mcp`, the MCP server an AI assistant starts over
  stdio. `flz mcp` reads one message per line and has each answered the way
  every `flz` verb is: on the controller through `answer/2`, as the token its
  options carry, or, while no controller can be reached, in a VM of its own
  through `offline/2`, which serves the knowledge tools and answers every
  operation `controller.unreachable`. Each prints its reply as one line on
  stdout, or nothing for a notification.

  The caller's source is `:mcp`, so an assistant reaches only what one over
  HTTP does, however the shell that started it could reach the controller.
  """

  alias Flezha.Error
  alias Flezha.Service.Admin
  alias Flezha.Service.CallerContext

  @doc """
  Answer `message` on the controller, as the caller `opts` names. A token
  given that does not verify refuses every operation with its own error, so a
  mistyped or expired one is not taken for no token at all.
  """
  @spec answer(binary(), keyword()) :: :ok
  def answer(message, opts) do
    with token when is_binary(token) <- Keyword.get(opts, :token),
         {:error, %Error{} = refused} <- CallerContext.from_token(:mcp, token) do
      serve(message, fn -> [] end, fn _, _ -> {:error, refused} end)
    else
      {:ok, context} -> as(message, context)
      nil -> as(message, %{source: :mcp})
    end
  end

  defp as(message, context),
    do:
      serve(
        message,
        fn -> Admin.catalogue(context) end,
        &Admin.call(&1, &2, context, :given),
        Map.get(context, :namespace) || ""
      )

  @doc """
  Answer `message` with no controller: the operations an assistant could
  reach are listed, and a call of one is refused naming the `socket` `flz`
  looked for.
  """
  @spec offline(binary(), String.t()) :: :ok
  def offline(message, socket) do
    unreachable = Error.new("controller.unreachable", socket: socket)

    serve(message, fn -> Admin.reachable(%{source: :mcp}) end, fn _, _ ->
      {:error, unreachable}
    end)
  end

  defp serve(message, catalogue, door, namespace \\ ""),
    do: message |> Flezha.Mcp.answer(nil, catalogue, door, namespace) |> print()

  defp print({_status, nil, _operation}), do: :ok
  defp print({_status, reply, _operation}), do: IO.puts(Jason.encode!(reply))
end
