defmodule Flezha.Service.OperativeManager.Teardown do
  @moduledoc """
  Stopping children through the hub and reclaiming what they leave on disk.
  """

  require Logger

  alias Flezha.Identificator
  alias Flezha.State.Completion
  alias Flezha.State.Paths

  # The ONE seam flezha stops children through: the hub's precommit removes
  # each declared-set entry BEFORE anything terminates (remove-before-terminate),
  # so a stop is remembered as list absence and the reconcile can never
  # resurrect it. The Overwatch's teardown — telling the runner to drop the
  # operative, plugin `:on_terminate`, table teardown — runs in its
  # `terminate/2` under the supervisor's shutdown. Best-effort callers discard
  # the verdict; a dead or mid-restart hub maps to an error, never a raise.
  def hub_stop_children(namespace, operative_ids, timeout) do
    result =
      try do
        Identificator.hub(namespace)
        |> ProcessHub.stop_children(operative_ids, awaitable: true, timeout: timeout)
        |> ProcessHub.Future.await()
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    case result do
      %ProcessHub.StopResult{status: :ok} -> :ok
      %ProcessHub.StopResult{errors: errors} -> {:error, errors || []}
      {:error, reason} -> {:error, reason}
    end
  end

  # On explicit stop, delete the on-disk state directories owned by the
  # operative. The operative_id is used as the operative_class identifier — the SDK
  # convention is that each operative's entity frames carry the operative_id
  # as operative_class. State files written under `app_data/state/<operative_id>/`
  # are removed; any procedure directory containing a `.tmp` or `.rewriting`
  # marker is skipped (with WARN) because a write is in flight and
  # truncating it would corrupt the rename ceremony.
  #
  # `child_unregistered_hook` deliberately does NOT call this — HotSwap
  # source-side and redistribution-away preserve files so the
  # destination (or next host) can pull them via WAL-follows-operative.
  def delete_operative_state_dir(operative_id) do
    # Retained or deleted, the writer processes go with the operative.
    Flezha.State.Supervisor.close_writers(operative_id)

    if Completion.exists?(operative_id) do
      # Finished workflow operative: retain its state + completion record for the
      # retention window so the outcome stays inspectable; `Flezha.State.Retention`
      # reclaims it once the window elapses.
      :ok
    else
      do_delete_operative_state_dir(operative_id)
    end
  end

  defp do_delete_operative_state_dir(operative_id) do
    class_dir = Paths.class_dir(operative_id)

    case File.ls(class_dir) do
      {:ok, procedure_names} ->
        Enum.each(procedure_names, fn procedure ->
          procedure_dir = Path.join(class_dir, procedure)
          maybe_delete_procedure(procedure_dir)
        end)

        _ = File.rmdir(class_dir)
        :ok

      {:error, :enoent} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp maybe_delete_procedure(procedure_dir) do
    case File.ls(procedure_dir) do
      {:ok, names} ->
        if Enum.any?(names, fn n ->
             String.ends_with?(n, ".tmp") or String.ends_with?(n, ".rewriting")
           end) do
          Logger.warning(
            "[operative_manager] skipping state dir #{procedure_dir}: write in flight " <>
              "(.tmp / .rewriting present)"
          )
        else
          _ = File.rm_rf(procedure_dir)
        end

      {:error, _} ->
        :ok
    end
  end
end
