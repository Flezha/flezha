defmodule Flezha.Service.CallerContext do
  @moduledoc """
  The context every service-API call carries as its last argument, and its
  validation.

  The shape is locked here so a caller — the CLI today, a UI later — is
  validated the same way by every service module. What a verb is authorized
  as, where, and how it is audited is declared once in `Flezha.AuthZ.Verbs`
  and applied by `Flezha.AuthZ.Verb`.
  """

  alias Flezha.AuthZ.TokenStore

  @valid_sources [:cli, :ui, :sdk, :http, :mcp, :internal]

  # The sources held to what the network may reach: a runner connection, HTTP,
  # and an AI assistant through `flz mcp`, which rides the controller's own
  # distribution but must reach no more than one over HTTP. The others run on
  # that distribution with the relay's full reach.
  @networked_sources [:sdk, :http, :mcp]

  @type t :: Flezha.AuthZ.caller_context()

  @doc """
  The context of a caller from `source` whose identity is the verified `token`:
  its principal, namespace and role, and nothing the caller wrote itself.
  """
  @spec from_token(atom(), binary()) :: {:ok, t()} | {:error, Flezha.Error.t()}
  def from_token(source, token) do
    with {:ok, %{principal: principal, namespace: namespace, role: role}} <-
           TokenStore.verify(token),
         do: {:ok, %{source: source, principal: principal, namespace: namespace, role: role}}
  end

  @doc "Whether the caller is held to what the network may reach: remote operations that do not stream."
  @spec networked?(t()) :: boolean()
  def networked?(caller_context), do: Map.get(caller_context, :source) in @networked_sources

  @spec validate(t()) :: :ok | {:error, :invalid_caller_context}
  def validate(%{source: src}) when src in @valid_sources, do: :ok
  def validate(_), do: {:error, :invalid_caller_context}

  @doc """
  Where a stream operation hands its events, and the process whose exit ends
  it, read from the context's `:emit`.

  A pid gets each event as `{:flezha_emit, event}` and is itself watched, so a
  consumer on another node receives plain messages and runs no code here. A
  function is called with each event in the streaming process, which watches
  the caller's group leader. With no `:emit`, events are dropped.
  """
  @spec emitter(t()) :: {(term() -> term()), pid()}
  def emitter(caller_context) do
    case Map.get(caller_context, :emit) do
      pid when is_pid(pid) ->
        {fn event ->
           send(pid, {:flezha_emit, event})
           :ok
         end, pid}

      emit when is_function(emit, 1) ->
        {emit, Process.group_leader()}

      nil ->
        {fn _event -> :ok end, Process.group_leader()}
    end
  end

  @doc """
  Hand each of `events` to `emit` in order: `:done` as soon as `emit` answers
  `:done`, `:continue` otherwise. The handler shape `receive_for/3` takes.
  """
  @spec hand_on([term()], (term() -> term())) :: :continue | :done
  def hand_on(events, emit) do
    if Enum.any?(events, &(emit.(&1) == :done)), do: :done, else: :continue
  end

  @doc """
  Hand every message this process receives to `handler` until `window_ms`
  (a positive integer or `:infinity`) elapses, `handler` answers `:done`, or
  `watch` exits. The one receive loop behind every streaming operation: the
  caller says what a message means, never when the stream ends. Answers
  `:done`, `:window_closed` or `:caller_down`. A window that has closed still
  hands on one message already queued, as `receive ... after 0` does, so a fed
  mailbox cannot keep it open.
  """
  @spec receive_for(integer() | :infinity, (term() -> :continue | :done), pid() | nil) ::
          :done | :window_closed | :caller_down
  def receive_for(window_ms, handler, watch \\ nil) do
    monitor = if watch, do: Process.monitor(watch)

    try do
      receive_until(deadline(window_ms), handler, monitor)
    after
      if monitor, do: Process.demonitor(monitor, [:flush])
    end
  end

  defp receive_until(deadline, handler, monitor) do
    remaining = remaining(deadline)

    receive do
      {:DOWN, ^monitor, :process, _pid, _reason} ->
        :caller_down

      message ->
        case handler.(message) do
          :continue when remaining == 0 -> :window_closed
          :continue -> receive_until(deadline, handler, monitor)
          :done -> :done
        end
    after
      remaining -> :window_closed
    end
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(window_ms), do: System.monotonic_time(:millisecond) + window_ms

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
