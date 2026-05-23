defmodule Flezha.Boot.StatePathMigrator do
  @moduledoc """
  One-shot, idempotent migration of the per-Partition state tree from the
  legacy `app_data/state/{executor_id}/{relay_class}/{shard_id}/` layout
  to the flat `app_data/state/{relay_class}/{shard_id}/` layout introduced
  by the `auto-restore-on-boot` change.

  Runs synchronously during `Flezha.Application.start/2` BEFORE
  `Flezha.State.Supervisor` opens any per-Partition `WalWriter`. After
  the first successful run, subsequent invocations are no-ops because
  the legacy directories are gone.

  Collision policy: when two executor-keyed copies of the same Partition
  exist (because the executor_id rotated and both old + new survived),
  the file with the higher last-modified time wins; the loser is
  preserved alongside as `<key>.<ext>.preserved-{old_executor_id}` so an
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
      # The namespace migration (`migrate_to_namespaced_layout/1`) is NOT
      # run here: the State subsystem readers (`Flezha.State.Registry`)
      # resolve the flat `{relay_class}/{shard}` layout and are not yet
      # namespace-aware, so moving state under `_system/` at boot would
      # orphan it. The namespace-state work wires it in once the readers
      # understand the prefix.
      do_migrate(root)
    else
      empty_result()
    end
  end

  @doc """
  One-shot pass that moves any top-level `{relay_class}/` subtree
  under `app_data/state/` into `app_data/state/_system/{relay_class}/`
  to satisfy the namespace prefix the platform-authn-authz change
  introduces.

  Idempotent: a second run on an already-migrated tree is a no-op
  and emits no warning. Safe to interrupt — each top-level
  `{relay_class}` tree is renamed atomically (single `rename(2)`
  call), so the post-condition is either entirely pre-namespace or
  entirely under `_system/`, never half-moved.
  """
  @spec migrate_to_namespaced_layout(binary()) :: :ok
  def migrate_to_namespaced_layout(root) do
    if File.dir?(root) do
      do_migrate_namespace(root)
    end

    :ok
  end

  defp do_migrate_namespace(root) do
    candidates =
      File.ls!(root)
      |> Enum.filter(&legacy_relay_class_dir?(root, &1))

    case candidates do
      [] ->
        :ok

      dirs ->
        system_dir = Path.join(root, "_system")
        File.mkdir_p!(system_dir)

        Logger.info(
          "[state_path_migrator] namespace migration: " <>
            "#{length(dirs)} top-level dir(s) → _system/"
        )

        Enum.each(dirs, fn relay_class ->
          from = Path.join(root, relay_class)
          to = Path.join(system_dir, relay_class)

          cond do
            File.exists?(to) ->
              Logger.warning(
                "[state_path_migrator] namespace migration skip: " <>
                  "#{to} already exists alongside legacy #{from}"
              )

            true ->
              case File.rename(from, to) do
                :ok ->
                  :ok

                {:error, reason} ->
                  Logger.warning(
                    "[state_path_migrator] namespace migration rename " <>
                      "#{from} -> #{to} failed: #{inspect(reason)}"
                  )
              end
          end
        end)

        :ok
    end
  end

  # A top-level entry is a "legacy relay-class dir" when it:
  #   * is a directory
  #   * is NOT one of the known namespace names (currently just _system,
  #     extended by `Flezha.AuthZ.NamespaceStore.list/0` once that
  #     subsystem is up, but at boot time we conservatively treat any
  #     non-`_system` dir that contains shard subdirs as legacy)
  #   * looks like a relay-class dir (contains at least one shard
  #     subdirectory).
  defp legacy_relay_class_dir?(root, name) do
    name != "_system" and File.dir?(Path.join(root, name)) and
      looks_like_relay_class?(Path.join(root, name)) and not known_namespace?(name)
  end

  defp looks_like_relay_class?(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.any?(entries, fn n -> File.dir?(Path.join(dir, n)) end)

      _ ->
        false
    end
  end

  defp known_namespace?(name) do
    # During boot, the NamespaceStore may not be available yet.
    # Default to "not a known namespace" so we err on the side of
    # treating a directory as legacy. The _system check above is
    # the only one we need at first-boot time.
    try do
      case Flezha.AuthZ.NamespaceStore.get(name) do
        {:ok, _} -> true
        :error -> false
      end
    rescue
      _ -> false
    catch
      _, _ -> false
    end
  end

  defp do_migrate(root) do
    legacy_dirs =
      File.ls!(root)
      |> Enum.filter(&legacy_executor_dir?(root, &1))

    case legacy_dirs do
      [] ->
        empty_result()

      dirs ->
        Logger.info(
          "[state_path_migrator] migrating #{length(dirs)} legacy executor-keyed dir(s)"
        )

        result = Enum.reduce(dirs, empty_result(), &migrate_executor_dir(root, &1, &2))

        Logger.info(
          "[state_path_migrator] done: renamed=#{result.renamed} collisions=#{result.collisions}"
        )

        result
    end
  end

  defp legacy_executor_dir?(root, name) do
    full = Path.join(root, name)

    File.dir?(full) and looks_like_executor_id?(name) and contains_relay_class_subdir?(full)
  end

  defp looks_like_executor_id?(name) do
    String.match?(name, ~r/^[A-Za-z0-9_-]{6,}$/)
  end

  defp contains_relay_class_subdir?(dir) do
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

  defp migrate_executor_dir(root, executor_id, acc) do
    legacy_root = Path.join(root, executor_id)

    File.ls!(legacy_root)
    |> Enum.reduce(acc, fn relay_class, acc ->
      legacy_rc = Path.join(legacy_root, relay_class)

      if File.dir?(legacy_rc) do
        File.ls!(legacy_rc)
        |> Enum.reduce(acc, fn shard_id, acc ->
          legacy_shard = Path.join(legacy_rc, shard_id)

          if File.dir?(legacy_shard) do
            migrate_shard(executor_id, relay_class, shard_id, legacy_shard, root, acc)
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

  defp migrate_shard(executor_id, relay_class, shard_id, legacy_shard, root, acc) do
    flat_shard = Path.join([root, relay_class, shard_id])
    File.mkdir_p!(flat_shard)

    File.ls!(legacy_shard)
    |> Enum.reduce(acc, fn name, acc ->
      from = Path.join(legacy_shard, name)
      to = Path.join(flat_shard, name)
      move_file(from, to, executor_id, acc)
    end)
    |> tap(fn _ ->
      _ = remove_if_empty(legacy_shard)
    end)
  end

  defp move_file(from, to, executor_id, acc) do
    cond do
      File.dir?(from) ->
        acc

      File.exists?(to) ->
        resolve_collision(from, to, executor_id, acc)

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

  defp resolve_collision(from, to, executor_id, acc) do
    {keep, drop} =
      case {mtime(from), mtime(to)} do
        {{:ok, t_from}, {:ok, t_to}} when t_from > t_to -> {from, to}
        {{:ok, t_from}, {:ok, t_to}} when t_from < t_to -> {to, from}
        _ -> {to, from}
      end

    preserved = preserved_path(drop, executor_id)

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

  defp preserved_path(path, executor_id) do
    path <> ".preserved-" <> executor_id
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
