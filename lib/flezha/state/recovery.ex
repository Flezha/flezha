defmodule Flezha.State.Recovery do
  @moduledoc """
  Single recovery primitive for per-Partition persistence: re-run the
  `RestoreInitState` handshake so a desynced or stalled Partition resumes
  from a consistent point.

  Used by the three failure edges that were previously silent:

    * a seq gap detected on append (`Flezha.Executor.StreamChannel`),
    * an output-staleness or compaction timeout
      (`Flezha.State.Watchdog`).

  Telemetry for *why* a resync was triggered is emitted by the caller;
  this module owns only the resync mechanism. The dispatch can be
  redirected in tests via the `:state_resync_fun` application env (an
  arity-2 fun `(partition_id, opts -> term)`), which lets a test simulate
  the SDK's rewind-and-resend without a live executor.
  """

  require Logger

  alias Flezha.Relay.Overwatch

  @spec resync(tuple(), keyword()) :: {:ok, reference()} | {:error, term()}
  def resync({_rc, _sh, _pk} = partition_id, opts \\ []) do
    case Application.get_env(:flezha, :state_resync_fun) do
      fun when is_function(fun, 2) -> fun.(partition_id, opts)
      _ -> do_resync(partition_id, opts)
    end
  end

  @doc """
  Resolve the `executor_id` currently owning `relay_class` from the
  ProcessHub registry. Returns `{:ok, executor_id}` or `{:error, term}`.
  """
  @spec resolve_executor(tuple()) :: {:ok, binary()} | {:error, term()}
  def resolve_executor({rc, _sh, _pk}) do
    case Flezha.Service.HubManager.get_registry("default") do
      {:ok, registry} ->
        match =
          Enum.find_value(registry, fn
            {_relay_id, %{metadata: %{tag: tag, relay: %{relay_class: ^rc}}}}
            when is_binary(tag) ->
              tag

            _ ->
              nil
          end)

        case match do
          nil -> {:error, "no active executor owns relay_class=#{rc}"}
          eid -> {:ok, eid}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_resync(partition_id, opts) do
    reason = Keyword.get(opts, :reason, :unknown)

    resolved =
      case Keyword.get(opts, :executor_id) do
        eid when is_binary(eid) -> {:ok, eid}
        _ -> resolve_executor(partition_id)
      end

    case resolved do
      {:ok, executor_id} ->
        case Overwatch.dispatch_restore_init_state(executor_id, partition_id) do
          {:ok, _ref} = ok ->
            ok

          other ->
            Logger.warning(
              "[State.Recovery] resync dispatch failed reason=#{inspect(reason)} " <>
                "partition=#{inspect(partition_id)}: #{inspect(other)}"
            )

            other
        end

      {:error, why} = err ->
        Logger.warning(
          "[State.Recovery] resync skipped reason=#{inspect(reason)} " <>
            "partition=#{inspect(partition_id)}: #{inspect(why)}"
        )

        err
    end
  end
end
