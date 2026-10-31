defmodule Flezha.State.Recovery do
  @moduledoc """
  Single recovery primitive for per-Cell persistence: pull the live Cell's
  state (`CompactCell`) as the log's baseline, so a desynced or stalled Cell
  resumes from a consistent point. It never pushes: a runner Cell is re-seated
  only by a bind or a correction, since a copy behind the live Cell would
  rewind it.

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
  alias Flezha.Process.Cspec
  alias Flezha.Service.HubManager
  alias Flezha.State.Commands

  @spec resync(tuple(), keyword()) :: {:ok, reference()} | {:error, term()}
  def resync({_rc, _sh, _pk} = cell_id, opts \\ []) do
    case Config.get(:state_resync_fun) do
      fun when is_function(fun, 2) -> fun.(cell_id, opts)
      _ -> do_resync(cell_id, opts)
    end
  end

  @doc """
  Resolve the `runner_id` currently owning `relay_class` from the
  ProcessHub registry. Returns `{:ok, runner_id}` or `{:error, term}`.

  The CLI `flz state rotate` surface is its only caller.
  """
  @spec resolve_runner(tuple()) :: {:ok, binary()} | {:error, term()}
  def resolve_runner({rc, _sh, _pk}) do
    with {:ok, bound} <- bound_runners() do
      case Map.fetch(bound, rc) do
        {:ok, eid} -> {:ok, eid}
        :error -> {:error, {:no_owning_runner, rc}}
      end
    end
  end

  @doc """
  The runner every relay_class is bound to now, from the tags in every running
  namespace's ProcessHub registry. A Cell's writer names the runner that last
  wrote it, which an idle Cell keeps naming after that runner has reconnected;
  this is the one to ask.
  """
  @spec bound_runners() :: {:ok, %{binary() => binary()}} | {:error, term()}
  def bound_runners() do
    HubManager.get_running_namespaces()
    |> Enum.reduce_while({:ok, %{}}, fn namespace, {:ok, bound} ->
      case HubManager.get_registry(namespace) do
        {:ok, registry} -> {:cont, {:ok, bound_in(registry, bound)}}
        # Stopped since the listing: it binds no runner.
        {:error, {:hub_down, ^namespace}} -> {:cont, {:ok, bound}}
        error -> {:halt, error}
      end
    end)
  end

  defp bound_in(registry, bound) do
    for {_relay_id, entry} <- registry,
        {cspec, _node_pids, metadata} = normalize_entry(entry),
        tag = Map.get(metadata, :tag),
        is_binary(tag),
        rc = relay_class(cspec),
        rc != nil,
        into: bound,
        do: {rc, tag}
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

  defp dispatch_resync(cell_id, runner_id, reason) do
    case Commands.dispatch_compact_cell(runner_id, cell_id) do
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

  # The durable registry stores `{relay_id, {cspec, node_pids, metadata}}` (a
  # pre-namespace row may omit metadata). The owning runner is `metadata.tag`;
  # the relay_class is the relay's `:relay_class` opt (default: its id), carried
  # in the cspec's start args — mirror of `Reconciler.Probe.relay_from_cspec/1`.
  defp normalize_entry({cspec, node_pids, metadata}), do: {cspec, node_pids, metadata}
  defp normalize_entry({cspec, node_pids}), do: {cspec, node_pids, %{}}

  defp relay_class(cspec) do
    case Cspec.start_arg(cspec) do
      {:ok, arg} -> relay_class_from_arg(arg)
      :error -> nil
    end
  end

  defp relay_class_from_arg(arg) do
    case arg do
      {_namespace, relay} when is_map(relay) ->
        relay_class_of(relay)

      {_namespace, _runner_id, relay} when is_map(relay) ->
        relay_class_of(relay)

      _ ->
        nil
    end
  end

  defp relay_class_of(relay) do
    Keyword.get(Map.get(relay, :opts, []), :relay_class, Map.get(relay, :id))
  end
end
