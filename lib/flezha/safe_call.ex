defmodule Flezha.SafeCall do
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

      # On a timeout the target is alive but wedged — introspect it so the log
      # names WHERE it is stuck instead of forcing operators to infer the wedge
      # from the caller side. `:noproc` targets are already gone (nothing to probe).
      probe = if classified == :timeout, do: probe_target(server), else: %{}

      # The event name is a published surface (`docs/internals/convergence_loop.md`)
      # and is deliberately not derived from this module's name.
      :telemetry.execute(
        [:flezha, :convergence, :safe_call],
        %{count: 1},
        Map.merge(%{result: classified, server: server}, probe)
      )

      Logger.warning(
        "[convergence] bounded call failed server=#{inspect(server)} " <>
          "request=#{inspect(elem_tag(request))} reason=#{inspect(reason)}" <>
          format_probe(probe)
      )

      {:error, classified}
  end

  defp classify({:timeout, _}), do: :timeout
  defp classify({:noproc, _}), do: :noproc
  defp classify(:noproc), do: :noproc
  defp classify(reason), do: reason

  # Resolve the target (pid | name | {name, node}) to a pid and sample its
  # mailbox depth + current function. `current_function` is the high-signal
  # field: it shows `:gen.do_call/4` when the target is itself blocked on a
  # downstream call. `Process.info/2` only accepts a LOCAL pid — it raises
  # `ArgumentError` on a remote pid (and yields `nil` only for a dead local
  # one), so we introspect locally and report a remote pid as such.
  defp probe_target(server) do
    case GenServer.whereis(server) do
      pid when is_pid(pid) and node(pid) == node() ->
        case Process.info(pid, [:message_queue_len, :current_function]) do
          [{:message_queue_len, qlen}, {:current_function, cf}] ->
            %{target_pid: pid, target_queue_len: qlen, target_current_function: cf}

          nil ->
            %{target_pid: pid, target_status: :unavailable}
        end

      pid when is_pid(pid) ->
        %{target_pid: pid, target_status: :remote, target_node: node(pid)}

      _ ->
        %{target_status: :no_proc}
    end
  end

  defp format_probe(probe) when map_size(probe) == 0, do: ""

  defp format_probe(%{target_queue_len: qlen, target_current_function: cf}),
    do: " target_queue_len=#{qlen} target_current_function=#{inspect(cf)}"

  defp format_probe(probe), do: " target=#{inspect(probe)}"

  # Log only the message tag, never the full payload (frames can be large).
  defp elem_tag(request) when is_tuple(request) and tuple_size(request) > 0, do: elem(request, 0)
  defp elem_tag(request), do: request
end
