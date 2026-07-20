defmodule Flezha.Service.OperativeManager do
  @moduledoc """
  Manages the operative startings and stoppings.
  """

  require Logger

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Convergence.SafeCall
  alias Flezha.Operative.Builder, as: OperativeBuilder
  alias Flezha.Shutdown.Coordinator, as: ShutdownCoordinator

  def operative_registry(namespace) do
    case HubManager.get_registry(namespace) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_operative_ids(namespace) do
    operatives =
      namespace
      |> HubManager.get_all_operatives()
      |> Enum.map(&elem(&1, 0))

    {:ok, operatives}
  end

  def get_operative_pid(namespace, operative_id) do
    case HubManager.get_pid(namespace, operative_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "operative not found"}
      err -> err
    end
  end

  def get_overwatch_state(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        # `SafeCall.bounded` returns `{:ok, state} | {:error, reason}` — the same
        # success shape as before, but a wedged operative now yields a logged
        # `{:error, :timeout}` (with the target's mailbox/current-function) instead
        # of an unhandled exit.
        SafeCall.bounded(operative_pid, :get_overwatch_state, overwatch_call_timeout_ms())

      err ->
        err
    end
  end

  @doc """
  Starts a new operative to the runner.
  """
  @spec start_operative(String.t(), OperativeBuilder.t()) ::
          {:ok, map()} | {:error, String.t()}
  def start_operative(namespace, operative) do
    with :ok <- check_not_shutting_down(),
         :ok <- OperativeBuilder.validate_operative(namespace, operative),
         :ok <- validate_factory(namespace, operative.factory_id),
         :ok <- check_operative_quota(namespace),
         :ok <- check_segment_quota(namespace, operative) do
      case init_operative(namespace, operative) do
        {:ok, _} = ok ->
          _ = Flezha.AuthZ.Quotas.record_allocation(namespace, :max_operatives, 1)
          seg_count = segment_count(operative)

          if seg_count > 0 do
            _ =
              Flezha.AuthZ.Quotas.record_allocation(
                namespace,
                :max_segments_per_operative,
                seg_count
              )
          end

          ok

        other ->
          other
      end
    else
      {:error, :shutting_down} ->
        {:error, :shutting_down}

      {:error, {:quota_exceeded, _, _} = qerr} ->
        {:error, qerr}

      {:error, reason} when is_binary(reason) ->
        {:error, %{operative_id: operative.id, node: node(), reason: reason}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp check_operative_quota(namespace) do
    case Flezha.AuthZ.Quotas.check_allocation(namespace, :max_operatives, 1) do
      :ok -> :ok
      {:error, :namespace_unknown} -> :ok
      {:error, _} = err -> err
    end
  end

  defp check_segment_quota(namespace, operative) do
    requested = segment_count(operative)

    if requested > 0 do
      case Flezha.AuthZ.Quotas.check_allocation(namespace, :max_segments_per_operative, requested) do
        :ok -> :ok
        {:error, :namespace_unknown} -> :ok
        {:error, _} = err -> err
      end
    else
      :ok
    end
  end

  defp check_not_shutting_down() do
    if ShutdownCoordinator.shutting_down?(), do: {:error, :shutting_down}, else: :ok
  end

  defp validate_factory(namespace, factory_id) when is_binary(factory_id) do
    case FleetManager.runners_for_factory(namespace, factory_id) do
      [] -> {:error, "Factory with id #{factory_id} does not exist"}
      [_ | _] -> :ok
    end
  end

  defp validate_factory(_namespace, _factory_id), do: :ok

  @doc """
  Stops `operative_id` the way its kind expects.

  A bound `:bounded` operative is stopped **cooperatively**: a `BoundedStop`
  trips the SDK's cancellation token and the operative winds down to its own
  `BoundedCompleted` (status "cancelled"), which drives the single teardown via
  `ControlChannel.handle_bounded_completed/2`. Everything else (`:unbounded`, or
  a bounded operative not bound to a runner) takes the hard `stop_operative/2`
  teardown. An unreadable kind (not found / wedged) also falls back to the hard
  stop so `flz operative stop` always attempts removal.

  Returns `{:ok, :cooperative}` when a cooperative stop was signalled, `:ok` after
  a hard teardown, or `{:error, reason}`.
  """
  @spec request_stop(String.t(), String.t()) ::
          :ok | {:ok, :cooperative} | {:error, term()}
  def request_stop(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: %{kind: :bounded}, runner_id: runner_id}}
      when is_binary(runner_id) ->
        case ControlUnit.bounded_stop(runner_id, operative_id) do
          :ok -> {:ok, :cooperative}
          # No control unit to cooperate with — fall back to the hard teardown.
          {:error, _} -> stop_operative(namespace, operative_id)
        end

      _ ->
        stop_operative(namespace, operative_id)
    end
  end

  @doc """
  Hard teardown: stop the operative, disable its plugins, delete its on-disk
  state, and release its quota. Drives `RemOperative` (terminal) — the converged
  teardown for both an explicit stop and a `BoundedCompleted`.
  """
  @spec stop_operative(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_operative(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        # Read the segment count before the stop tears the operative down — the
        # operative is gone afterwards and the count is needed to release the
        # `:max_segments_per_operative` quota symmetrically with start.
        seg_count = operative_segment_count(namespace, operative_id)

        case GenServer.call(operative_pid, :stop_operative) do
          :ok ->
            _ = Flezha.Service.PluginManager.disable_operative_plugins(namespace, operative_id)
            _ = delete_operative_state_dir(operative_id)
            _ = Flezha.AuthZ.Quotas.record_deallocation(namespace, :max_operatives, 1)

            if seg_count > 0 do
              _ =
                Flezha.AuthZ.Quotas.record_deallocation(
                  namespace,
                  :max_segments_per_operative,
                  seg_count
                )
            end

            :ok

          {:error, reason} ->
            {:error, reason}
        end

      err ->
        err
    end
  end

  defp operative_segment_count(namespace, operative_id) do
    case get_overwatch_state(namespace, operative_id) do
      {:ok, %{builder: builder}} -> segment_count(builder)
      _ -> 0
    end
  end

  defp segment_count(operative) do
    case Map.get(operative, :segments) do
      ps when is_list(ps) -> length(ps)
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  def stop_all_operatives(namespace) do
    hub_id = Identificator.hub(namespace)
    operatives = HubManager.get_all_operatives(namespace)
    operative_ids = Enum.map(operatives, fn {id, _pid} -> id end)

    Enum.each(operative_ids, fn operative_id ->
      _ = Flezha.Service.PluginManager.disable_operative_plugins(namespace, operative_id)
    end)

    if operative_ids != [] do
      try do
        hub_id
        |> ProcessHub.stop_children(operative_ids, awaitable: true, timeout: 15_000)
        |> ProcessHub.Future.await()
      catch
        :exit, _ -> :ok
      end

      Enum.each(operative_ids, &delete_operative_state_dir/1)
    end

    :ok
  end

  # On explicit stop, delete the on-disk state directories owned by the
  # operative. The operative_id is used as the operative_class identifier — the SDK
  # convention is that each operative's segment frames carry the operative_id
  # as operative_class. State files written under `app_data/state/<operative_id>/`
  # are removed; any operation directory containing a `.tmp` or `.rewriting`
  # marker is skipped (with WARN) because a write is in flight and
  # truncating it would corrupt the rename ceremony.
  #
  # `child_unregistered_hook` deliberately does NOT call this — HotSwap
  # source-side and redistribution-away preserve files so the
  # destination (or next host) can pull them via WAL-follows-operative.
  defp delete_operative_state_dir(operative_id) do
    if Flezha.State.Completion.exists?(operative_id) do
      # Finished bounded operative: retain its state + completion record for the
      # retention window so the outcome stays inspectable; `Flezha.State.Retention`
      # reclaims it once the window elapses.
      :ok
    else
      do_delete_operative_state_dir(operative_id)
    end
  end

  defp do_delete_operative_state_dir(operative_id) do
    root = Flezha.State.Registry.state_root()
    class_dir = Path.join(root, operative_id)

    case File.ls(class_dir) do
      {:ok, operation_names} ->
        Enum.each(operation_names, fn operation ->
          operation_dir = Path.join(class_dir, operation)
          maybe_delete_operation(operation_dir)
        end)

        _ = File.rmdir(class_dir)
        :ok

      {:error, :enoent} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp maybe_delete_operation(operation_dir) do
    case File.ls(operation_dir) do
      {:ok, names} ->
        if Enum.any?(names, fn n ->
             String.ends_with?(n, ".tmp") or String.ends_with?(n, ".rewriting")
           end) do
          Logger.warning(
            "[operative_manager] skipping state dir #{operation_dir}: write in flight " <>
              "(.tmp / .rewriting present)"
          )
        else
          _ = File.rm_rf(operation_dir)
        end

      {:error, _} ->
        :ok
    end
  end

  def send_io(operative_id, namespace, message, metadata) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        GenServer.cast(operative_pid, {:io_in, message, metadata})

      err ->
        err
    end
  end

  @typedoc """
  What became of a payload handed to `send_io_confirming/5`. The three `:ok`
  shapes are terminal — the receiver has decided the payload needs no further
  delivery — and every error is retryable by the sender.
  """
  @type ingest_verdict ::
          :ok
          | {:ok, :absorbed}
          | {:ok, :suspended}
          | {:error, :not_ready | :not_found | :refused}

  @doc """
  Confirming counterpart to `send_io/4`: runs the target's single pre-delivery
  ingest path and answers what happened, so a forwarding sink can tell a
  terminal outcome from one worth retrying. An unresolvable target is
  `{:error, :not_found}` — also retryable, since the operative may not be
  started yet.
  """
  @spec send_io_confirming(String.t(), String.t(), binary(), map(), timeout()) :: ingest_verdict()
  def send_io_confirming(operative_id, namespace, message, metadata, timeout \\ 5_000) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        GenServer.call(operative_pid, {:ingest, message, metadata}, timeout)

      _err ->
        {:error, :not_found}
    end
  end

  def create_snapshot(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        GenServer.call(operative_pid, :create_snapshot)

      err ->
        err
    end
  end

  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  defp init_operative(namespace, operative, attempt \\ 0) do
    child_spec = %{
      id: operative.id,
      start: {
        Flezha.Operative.Overwatch,
        :start_link,
        [{namespace, operative}]
      },
      restart: :transient
    }

    result =
      Identificator.hub(namespace)
      |> ProcessHub.start_child(child_spec,
        awaitable: true,
        # We will get the errors from the start result.
        disable_logging: true,
        timeout: 15_000
      )
      |> ProcessHub.Future.await()

    # `:no_response` surfaces when ProcessHub's `with_partition_check`
    # short-circuits `execute/2` without sending a response — i.e. the
    # local hub's `dist_sup` was momentarily missing or dead, so
    # `Service.State.is_partitioned?/1` returned `true`. The dist_sup
    # restarts on its own; the only thing the caller can do is wait it
    # out. Retry up to @no_response_retry_attempts times with
    # exponential backoff (150, 300, 600, 1200, 2400, ... ms, capped at
    # 5s) so a transient partition during controller/cluster startup, a
    # snapshot/restore taking down `dist_sup`, or parallel-suite test
    # churn doesn't surface as a hard failure. Stable steady-state
    # `:no_response` (8 attempts, ~14s of wait) still propagates.
    cond do
      no_response?(result) and attempt < @no_response_retry_attempts ->
        backoff = min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)
        Process.sleep(backoff)
        init_operative(namespace, operative, attempt + 1)

      true ->
        finalize_init_operative(namespace, operative, result)
    end
  end

  defp no_response?(%ProcessHub.StartResult{errors: errors}) when is_list(errors) do
    Enum.any?(errors, fn
      {_cid, :no_response} -> true
      {_cid, {:no_response, _}} -> true
      {_cid, {:error, :no_response}} -> true
      _ -> false
    end)
  end

  defp no_response?(_result), do: false

  defp finalize_init_operative(namespace, operative, result) do
    case result do
      %ProcessHub.StartResult{} ->
        our_started =
          Enum.find(result.started || [], fn {cid, _} -> cid == operative.id end)

        case our_started do
          {_cid, [{operative_node, pid}]} ->
            # ProcessHub reports `started` as soon as `start_link/1`
            # returns; the operative's `handle_continue(:bind_runner, …)`
            # surfaces a plugin-chain failure via the `init_error` field
            # on Overwatch state — `:registry_runner_set` returns
            # `{:error, _}` instead of crashing the GenServer. The
            # `:exit` catch covers older failure modes (timeout, kill).
            try do
              case GenServer.call(pid, :registry_runner_set) do
                {:ok, runner_id} ->
                  {:ok,
                   %{
                     operative_id: operative.id,
                     runner_id: runner_id,
                     node: operative_node,
                     pid: pid
                   }}

                {:error, reason} ->
                  _ = stop_child_quietly(namespace, operative.id)
                  {:error, %{operative_id: operative.id, node: node(), reason: reason}}
              end
            catch
              # Started, then exited cleanly before we could read its binding —
              # a run-once bounded operative that already completed, or a concurrent stop. A
              # clean exit is a successful start, not a failure; no live binding
              # remains, so `runner_id` is nil.
              :exit, {reason, _} when reason in [:normal, :noproc] ->
                {:ok,
                 %{operative_id: operative.id, runner_id: nil, node: operative_node, pid: pid}}

              :exit, {reason, _} ->
                Logger.warning(
                  "Operative #{operative.id} appeared in `started` but died during " <>
                    "post-init continue (reason=#{inspect(reason)})"
                )

                _ = stop_child_quietly(namespace, operative.id)
                {:error, %{operative_id: operative.id, node: node(), reason: reason}}
            end

          _ ->
            Logger.warning(
              "Operative #{operative.id} failed to start. " <>
                "Started: #{inspect(result.started)}, Errors: #{inspect(result.errors)}, " <>
                "Status: #{inspect(result.status)}"
            )

            reason =
              case Enum.find(result.errors || [], fn {cid, _} -> cid == operative.id end) do
                {_cid, {:error, reason}} -> reason
                {_cid, {reason, _child_spec}} -> reason
                {_cid, reason} -> reason
                _ -> :unknown
              end

            _ = stop_child_quietly(namespace, operative.id)
            {:error, %{operative_id: operative.id, node: node(), reason: reason}}
        end

      {:error, reason} ->
        {:error, %{operative_id: operative.id, node: node(), reason: reason}}
    end
  end

  defp stop_child_quietly(namespace, operative_id) do
    try do
      Identificator.hub(namespace)
      |> ProcessHub.stop_children([operative_id], awaitable: true, timeout: 5_000)
      |> ProcessHub.Future.await()
    catch
      _, _ -> :ok
    end
  end

  def exist?(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def get_segment_state(namespace, operative_id) do
    case get_operative_pid(namespace, operative_id) do
      {:ok, operative_pid} ->
        # Unwrap one level: the operative's own reply is already `{:ok, json} |
        # {:error, reason}`, so collapse `SafeCall`'s outer `{:ok, _}` to preserve
        # the existing contract; a bounded timeout/noproc stays `{:error, reason}`.
        case SafeCall.bounded(operative_pid, :get_segment_state, overwatch_call_timeout_ms()) do
          {:ok, reply} -> reply
          {:error, _} = err -> err
        end

      err ->
        err
    end
  end

  defp overwatch_call_timeout_ms,
    do: Application.get_env(:flezha, :overwatch_call_timeout_ms, 5_000)

  @doc """
  Pick an active compute engine for `operative_id` in `namespace`.

  Placement is deterministic — rendezvous (HRW) hashing of `operative_id` over
  the candidate runners — so every controller and every tick computes the
  same target with no coordination, load spreads evenly, and only ~1/N
  operatives move when the runner set changes. A newly connected runner
  never reshuffles existing operatives: placement is computed only at start /
  forced rebind.

  When `factory_id` is given the pool is restricted to runners advertising
  that factory. Without this filter a mixed-factory namespace (e.g.
  market-operative owning `crypto_trades` + market-app owning
  `market-init-state`) would pick the wrong node, routing every input frame
  to a process that doesn't exist on the foreign node (the Java SDK surfaces
  this as `DefaultOperativeDispatcher.enqueue: operative 'X' not found`). Returns
  `nil` when no candidate exists.
  """
  @spec assign_runner(String.t(), binary() | nil, binary()) :: binary() | nil
  def assign_runner(namespace, factory_id, operative_id) do
    namespace
    |> candidate_runners(factory_id)
    |> pick_runner(operative_id)
  end

  defp candidate_runners(namespace, nil), do: FleetManager.active_runners(namespace)

  defp candidate_runners(namespace, factory_id) when is_binary(factory_id),
    do: FleetManager.runners_for_factory(namespace, factory_id)

  # The candidate set is the authority: a runner is in it only while its
  # transport connection is open. HRW then picks the runner with the
  # highest hash of `{operative_id, runner}` — deterministic and stable under
  # membership change.
  @doc false
  def pick_runner([], _operative_id), do: nil

  def pick_runner(runner_ids, operative_id),
    do: Enum.max_by(runner_ids, fn ce -> :erlang.phash2({operative_id, ce}) end)
end
