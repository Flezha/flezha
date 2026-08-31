defmodule Flezha.Boot.StatePathMigrator do
  @moduledoc """
  One-shot, idempotent migrations of the per-Entity state tree, oldest
  layout first:

    1. Legacy `{runner_id}/{operative_class}/{procedure_id}/` → flat
       `{operative_class}/{procedure_id}/` (`auto-restore-on-boot`).
    2. Flat `{operative_class}/...` → namespaced
       `_system/{operative_class}/...` (`finish-platform-authn-authz`) —
       one atomic `rename/2` per class dir, so an interrupted pass leaves
       whole classes either moved or not, and a re-run moves the rest.

  Runs synchronously during `Flezha.Application.start/2` BEFORE
  `Flezha.State.Supervisor` opens any per-Entity `WalWriter`. The
  `.layout` marker at the state root (written after a completed pass 2)
  is what makes later boots skip both passes — the two legacy layouts and
  the namespaced one are structurally indistinguishable (`ns/class/proc`
  and `runner/class/proc` are both three levels), so presence of the
  marker, not shape, is the discriminator.

  Collision policy for pass 1: when two runner-keyed copies of the same
  Entity exist (because the runner_id rotated and both old + new
  survived), the file with the higher last-modified time wins; the loser
  is preserved alongside as `<key>.<ext>.preserved-{old_runner_id}` so an
  operator can inspect it.
  """

  alias Flezha.State.Paths

  require Logger

  @layout_marker ".layout"
  @layout_version "v3-namespaced"

  @type result :: %{
          renamed: non_neg_integer(),
          collisions: non_neg_integer(),
          preserved_files: [binary()]
        }

  @spec migrate() :: result()
  def migrate() do
    root = Paths.state_root()

    cond do
      not File.dir?(root) ->
        # A fresh install starts on the namespaced layout: stamp it now, so a
        # later boot can never mistake a namespace dir for a legacy one. An
        # uncreatable root stays a no-op (the state subsystem will surface
        # that on its own terms).
        with :ok <- File.mkdir_p(root) do
          _ = File.write(Path.join(root, @layout_marker), @layout_version)
        end

        empty_result()

      File.exists?(Path.join(root, @layout_marker)) ->
        empty_result()

      true ->
        result = do_migrate(root)
        namespace_moves = migrate_to_namespace_layout(root)
        File.write!(Path.join(root, @layout_marker), @layout_version)

        %{
          result
          | renamed: result.renamed + namespace_moves,
            preserved_files: Enum.map(result.preserved_files, &lifted_path(root, &1))
        }
    end
  end

  # Pass 1 records preserved files at their flat location; pass 2 lifts the
  # class dirs holding them, so the reported paths follow the move.
  defp lifted_path(root, path) do
    if File.exists?(path) do
      path
    else
      lifted = Path.join([root, "_system", Path.relative_to(path, root)])
      if File.exists?(lifted), do: lifted, else: path
    end
  end

  # Pass 2: lift every remaining top-level class dir under `_system/`. At
  # this point the top level can only hold flat class dirs (pass 1 consumed
  # the runner-keyed ones) and — after an interrupted earlier attempt —
  # `_system` itself, which is skipped by name.
  defp migrate_to_namespace_layout(root) do
    system_dir = Path.join(root, "_system")

    movable =
      File.ls!(root)
      |> Enum.filter(fn name ->
        name != "_system" and not String.starts_with?(name, ".") and
          File.dir?(Path.join(root, name))
      end)

    if movable != [] do
      File.mkdir_p!(system_dir)
      Logger.info("[state_path_migrator] moving #{length(movable)} class dir(s) under _system/")
    end

    Enum.count(movable, fn class ->
      case File.rename(Path.join(root, class), Path.join(system_dir, class)) do
        :ok ->
          true

        {:error, reason} ->
          Logger.warning(
            "[state_path_migrator] moving #{class} under _system failed: #{inspect(reason)}"
          )

          false
      end
    end)
  end

  defp do_migrate(root) do
    legacy_dirs =
      File.ls!(root)
      |> Enum.filter(&legacy_runner_dir?(root, &1))

    case legacy_dirs do
      [] ->
        empty_result()

      dirs ->
        Logger.info("[state_path_migrator] migrating #{length(dirs)} legacy runner-keyed dir(s)")

        result = Enum.reduce(dirs, empty_result(), &migrate_runner_dir(root, &1, &2))

        Logger.info(
          "[state_path_migrator] done: renamed=#{result.renamed} collisions=#{result.collisions}"
        )

        result
    end
  end

  defp legacy_runner_dir?(root, name) do
    full = Path.join(root, name)

    name != "_system" and File.dir?(full) and looks_like_runner_id?(name) and
      contains_operative_class_subdir?(full)
  end

  defp looks_like_runner_id?(name) do
    String.match?(name, ~r/^[A-Za-z0-9_-]{6,}$/)
  end

  defp contains_operative_class_subdir?(dir) do
    case File.ls(dir) do
      {:ok, names} ->
        Enum.any?(names, fn n ->
          rc_dir = Path.join(dir, n)
          File.dir?(rc_dir) and File.ls!(rc_dir) |> Enum.any?(&File.dir?(Path.join(rc_dir, &1)))
        end)

      {:error, _} ->
        false
    end
  end

  defp migrate_runner_dir(root, runner_id, acc) do
    legacy_root = Path.join(root, runner_id)

    File.ls!(legacy_root)
    |> Enum.reduce(acc, fn operative_class, acc ->
      legacy_rc = Path.join(legacy_root, operative_class)

      if File.dir?(legacy_rc) do
        File.ls!(legacy_rc)
        |> Enum.reduce(acc, fn procedure_id, acc ->
          legacy_procedure = Path.join(legacy_rc, procedure_id)

          if File.dir?(legacy_procedure) do
            migrate_procedure(
              runner_id,
              operative_class,
              procedure_id,
              legacy_procedure,
              root,
              acc
            )
          else
            acc
          end
        end)
        |> tap(fn _ ->
          _ = remove_if_empty(legacy_rc)
        end)
      else
        acc
      end
    end)
    |> tap(fn _ ->
      _ = remove_if_empty(legacy_root)
    end)
  end

  defp migrate_procedure(runner_id, operative_class, procedure_id, legacy_procedure, root, acc) do
    flat_procedure = Path.join([root, operative_class, procedure_id])
    File.mkdir_p!(flat_procedure)

    File.ls!(legacy_procedure)
    |> Enum.reduce(acc, fn name, acc ->
      from = Path.join(legacy_procedure, name)
      to = Path.join(flat_procedure, name)
      move_file(from, to, runner_id, acc)
    end)
    |> tap(fn _ ->
      _ = remove_if_empty(legacy_procedure)
    end)
  end

  defp move_file(from, to, runner_id, acc) do
    cond do
      File.dir?(from) ->
        acc

      File.exists?(to) ->
        resolve_collision(from, to, runner_id, acc)

      true ->
        case File.rename(from, to) do
          :ok ->
            %{acc | renamed: acc.renamed + 1}

          {:error, reason} ->
            Logger.warning(
              "[state_path_migrator] rename #{from} -> #{to} failed: #{inspect(reason)}"
            )

            acc
        end
    end
  end

  defp resolve_collision(from, to, runner_id, acc) do
    {keep, _drop} =
      case {mtime(from), mtime(to)} do
        {{:ok, t_from}, {:ok, t_to}} when t_from > t_to -> {from, to}
        {{:ok, t_from}, {:ok, t_to}} when t_from < t_to -> {to, from}
        _ -> {to, from}
      end

    # The preserved sibling always lands BESIDE the winner (the flat/`to`
    # location) so it travels with its class dir through the namespace pass
    # instead of stranding the legacy runner dir.
    preserved = preserved_path(to, runner_id)

    cond do
      keep == to ->
        case File.rename(from, preserved) do
          :ok ->
            Logger.info("[state_path_migrator] collision: preserved #{from} as #{preserved}")

            %{
              acc
              | collisions: acc.collisions + 1,
                preserved_files: [preserved | acc.preserved_files]
            }

          {:error, reason} ->
            Logger.warning(
              "[state_path_migrator] preserve #{from} as #{preserved} failed: #{inspect(reason)}"
            )

            acc
        end

      true ->
        with :ok <- File.rename(to, preserved),
             :ok <- File.rename(from, to) do
          Logger.info("[state_path_migrator] collision: preserved #{to} as #{preserved}")

          %{
            acc
            | renamed: acc.renamed + 1,
              collisions: acc.collisions + 1,
              preserved_files: [preserved | acc.preserved_files]
          }
        else
          {:error, reason} ->
            Logger.warning(
              "[state_path_migrator] collision swap of #{to} <-> #{from} failed: #{inspect(reason)}"
            )

            acc
        end
    end
  end

  defp preserved_path(path, runner_id) do
    path <> ".preserved-" <> runner_id
  end

  defp mtime(path) do
    case File.stat(path, time: :posix) do
      {:ok, %File.Stat{mtime: mtime}} -> {:ok, mtime}
      {:error, _} = err -> err
    end
  end

  defp remove_if_empty(dir) do
    case File.ls(dir) do
      {:ok, []} -> File.rmdir(dir)
      _ -> :ok
    end
  end

  defp empty_result(), do: %{renamed: 0, collisions: 0, preserved_files: []}
end
