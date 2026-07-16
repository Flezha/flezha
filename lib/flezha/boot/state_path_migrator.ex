defmodule Flezha.Boot.StatePathMigrator do
  @moduledoc """
  One-shot, idempotent migration of the per-Segment state tree from the
  legacy `app_data/state/{runner_id}/{operative_class}/{operation_id}/` layout
  to the flat `app_data/state/{operative_class}/{operation_id}/` layout introduced
  by the `auto-restore-on-boot` change.

  Runs synchronously during `Flezha.Application.start/2` BEFORE
  `Flezha.State.Supervisor` opens any per-Segment `WalWriter`. After
  the first successful run, subsequent invocations are no-ops because
  the legacy directories are gone.

  Collision policy: when two runner-keyed copies of the same Segment
  exist (because the runner_id rotated and both old + new survived),
  the file with the higher last-modified time wins; the loser is
  preserved alongside as `<key>.<ext>.preserved-{old_runner_id}` so an
  operator can inspect it.
  """

  alias Flezha.State.Registry

  require Logger

  @type result :: %{
          renamed: non_neg_integer(),
          collisions: non_neg_integer(),
          preserved_files: [binary()]
        }

  @spec migrate() :: result()
  def migrate() do
    root = Registry.state_root()

    if File.dir?(root) do
      # Only the legacy runner-keyed → flat migration runs. The reader
      # (`Flezha.State.Registry`) resolves the flat `{operative_class}/{operation}`
      # layout, so the migrator must produce no other layout — a namespace
      # prefix added ahead of reader support would orphan state.
      do_migrate(root)
    else
      empty_result()
    end
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

    File.dir?(full) and looks_like_runner_id?(name) and contains_operative_class_subdir?(full)
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
        |> Enum.reduce(acc, fn operation_id, acc ->
          legacy_operation = Path.join(legacy_rc, operation_id)

          if File.dir?(legacy_operation) do
            migrate_operation(
              runner_id,
              operative_class,
              operation_id,
              legacy_operation,
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

  defp migrate_operation(runner_id, operative_class, operation_id, legacy_operation, root, acc) do
    flat_operation = Path.join([root, operative_class, operation_id])
    File.mkdir_p!(flat_operation)

    File.ls!(legacy_operation)
    |> Enum.reduce(acc, fn name, acc ->
      from = Path.join(legacy_operation, name)
      to = Path.join(flat_operation, name)
      move_file(from, to, runner_id, acc)
    end)
    |> tap(fn _ ->
      _ = remove_if_empty(legacy_operation)
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
    {keep, drop} =
      case {mtime(from), mtime(to)} do
        {{:ok, t_from}, {:ok, t_to}} when t_from > t_to -> {from, to}
        {{:ok, t_from}, {:ok, t_to}} when t_from < t_to -> {to, from}
        _ -> {to, from}
      end

    preserved = preserved_path(drop, runner_id)

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
