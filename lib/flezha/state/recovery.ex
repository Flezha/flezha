defmodule Flezha.State.Recovery do
  @moduledoc """
  Single recovery primitive for per-Cell persistence: re-run the
  `RestoreInitState` handshake so a desynced or stalled Cell resumes
  from a consistent point.

  Used by the failure edges that are otherwise silent:

    * a seq gap detected on append (`Flezha.Runner.StreamChannel`),
    * an output-staleness or compaction timeout
      (`Flezha.State.Watchdog`).

  Telemetry for *why* a resync was triggered is emitted by the caller;
  this module owns only the resync mechanism. The dispatch can be
  redirected in tests via the `:state_resync_fun` application env (an
  arity-2 fun `(cell_id, opts -> term)`), which lets a test simulate
  the SDK's rewind-and-resend without a live runner.
  """

  require Logger

  alias Flezha.Config
  alias Flezha.Operative.Replay
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager
  alias Flezha.State.Commands
  alias Flezha.State.Restore

  @spec resync(tuple(), keyword()) :: {:ok, reference()} | {:error, term()}
  def resync({_rc, _sh, _pk} = cell_id, opts \\ []) do
    case Config.get(:state_resync_fun) do
      fun when is_function(fun, 2) -> fun.(cell_id, opts)
      _ -> do_resync(cell_id, opts)
    end
  end

  @doc """
  Bind-driven WAL replay trigger. Called by `Flezha.Operative.Overwatch`
  from `handle_continue(:after_bind, _)` with the Overwatch's own
  `runner_id` so the replay never depends on a class→runner lookup
  in the durable registry (the lookup that failed on cold boot and
  produced the silent "resync skipped reason=:output_stall" no-op).

  This is the single state-restoration trigger for non-HotSwap operatives;
  every Overwatch bind unconditionally invokes it. The implementation
  is intentionally trivial — the heavy lifting lives in
  `Flezha.State.Commands.dispatch_restore_init_state/2`.
  """
  @spec resync_for_overwatch(pid(), binary()) :: :ok
  def resync_for_overwatch(overwatch_pid, runner_id)
      when is_pid(overwatch_pid) and is_binary(runner_id) do
    Replay.kick_restore_for_bound_runner(overwatch_pid, runner_id)
    :ok
  end

  @doc """
  Resolve the `runner_id` currently owning `operative_class` from the
  ProcessHub registry. Returns `{:ok, runner_id}` or `{:error, term}`.

  The CLI `flz state rotate` surface is its only caller; the resync path
  resolves its runner through `resync_for_overwatch/2` instead.
  """
  @spec resolve_runner(tuple()) :: {:ok, binary()} | {:error, term()}
  def resolve_runner({rc, _sh, _pk}) do
    case HubManager.get_registry("default") do
      {:ok, registry} ->
        match =
          Enum.find_value(registry, fn {_operative_id, entry} ->
            {cspec, _node_pids, metadata} = normalize_entry(entry)
            tag = Map.get(metadata, :tag)

            if is_binary(tag) and operative_class(cspec) == rc, do: tag
          end)

        case match do
          nil -> {:error, "no active runner owns operative_class=#{rc}"}
          eid -> {:ok, eid}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_resync(cell_id, opts) do
    reason = Keyword.get(opts, :reason, :unknown)
    runner_id = Keyword.get(opts, :runner_id)

    cond do
      is_binary(runner_id) ->
        dispatch_resync(cell_id, runner_id, reason)

      true ->
        # Callers without an runner_id are only signalling "I observed
        # a stall"; the convergence Loop's rebind heal owns the recovery
        # action. This module does not dispatch one out-of-band.
        {:error, :no_runner_id}
    end
  end

  # Direction is chosen by who holds authoritative history: with a local
  # baseline the controller pushes `RestoreInitState`; without one it pulls a
  # full-state snapshot via `CompactCell` — an empty RestoreInitState
  # would rebuild the live Cell to `init_state` and wipe accumulated state.
  defp dispatch_resync(cell_id, runner_id, reason) do
    result =
      if Restore.has_persisted_state?(cell_id) do
        Commands.dispatch_restore_init_state(runner_id, cell_id)
      else
        Commands.dispatch_compact_cell(runner_id, cell_id)
      end

    case result do
      {:ok, _ref} = ok ->
        ok

      other ->
        Logger.warning(
          "[State.Recovery] resync dispatch failed reason=#{inspect(reason)} " <>
            "cell=#{inspect(cell_id)}: #{inspect(other)}"
        )

        other
    end
  end

  # The durable registry stores `{operative_id, {cspec, node_pids, metadata}}` (a
  # pre-namespace row may omit metadata). The owning runner is `metadata.tag`;
  # the operative_class is the operative's `:operative_class` opt (default: its id), carried
  # in the cspec's start args — mirror of `Reconciler.Probe.operative_from_cspec/1`.
  defp normalize_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  defp operative_class(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, arg} -> operative_class_from_arg(arg)
      :error -> nil
    end
  end

  defp operative_class_from_arg(arg) do
    case arg do
      {_namespace, operative} when is_map(operative) ->
        operative_class_of(operative)

      {_namespace, _runner_id, operative} when is_map(operative) ->
        operative_class_of(operative)

      _ ->
        nil
    end
  end

  defp operative_class_of(operative) do
    Keyword.get(Map.get(operative, :opts, []), :operative_class, Map.get(operative, :id))
  end
end
