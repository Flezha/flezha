defmodule Flezha.Convergence.SafeCall do
  @moduledoc """
  One bounded `GenServer.call` wrapper for calls into a process that may
  be wedged, mid-restart, or already gone.

  `bounded/3` returns `{:ok, reply}` on success and `{:error, reason}`
  on timeout / `:noproc` / any other exit, after logging once and
  emitting `[:flezha, :convergence, :safe_call]` telemetry. Callers that
  only care about side effects ignore the result; callers that need the
  reply pattern-match it.
  """

  require Logger

  @type target :: pid() | atom() | {atom(), node()}

  @spec bounded(target(), term(), timeout()) :: {:ok, term()} | {:error, term()}
  def bounded(server, request, timeout) when is_integer(timeout) or timeout == :infinity do
    {:ok, GenServer.call(server, request, timeout)}
  catch
    :exit, reason ->
      classified = classify(reason)

      :telemetry.execute(
        [:flezha, :convergence, :safe_call],
        %{count: 1},
        %{result: classified, server: server}
      )

      Logger.warning(
        "[convergence] bounded call failed server=#{inspect(server)} " <>
          "request=#{inspect(elem_tag(request))} reason=#{inspect(reason)}"
      )

      {:error, classified}
  end

  defp classify({:timeout, _}), do: :timeout
  defp classify({:noproc, _}), do: :noproc
  defp classify(:noproc), do: :noproc
  defp classify(reason), do: reason

  # Log only the message tag, never the full payload (frames can be large).
  defp elem_tag(request) when is_tuple(request) and tuple_size(request) > 0, do: elem(request, 0)
  defp elem_tag(request), do: request
end
