defmodule Flezha.State.Paths do
  require Logger

  @moduledoc """
  Path resolution for per-Cell WAL, snapshot, and outbox-dedup files.

  All persistence files live under
  `<state_root>/<namespace>/<operative_class>/<tasklet_id>/`, where
  `<state_root>` is the `:flezha, :state_root` application env (set by tests
  for isolation), else `app_data/state`. The operator-facing knob is the
  `FLEZHA_STATE_DIR` OS env, which fills that key once at boot in
  `config/runtime.exs`; nothing reads the variable afterwards, so a test that
  sets it in a live BEAM has moved no writer. `namespace_dir/1` and
  `class_dir/1` own the first two levels; nothing outside this module assembles
  them.

  `cell_key` is URL-safe-base64-encoded when it contains characters
  that would be unsafe on POSIX filesystems (anything outside `[A-Za-z0-9_.-]`).
  `decode_cell_key/1` reverses the encoding.
  """

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.State.Durable

  @type cell_id ::
          {operative_class :: binary(), tasklet_id :: binary(), cell_key :: binary()}

  @type scan_result :: %{
          wal_files: [{cell_key :: binary(), path :: binary()}],
          snapshot_files: [
            {cell_key :: binary(), seq :: non_neg_integer(), path :: binary()}
          ],
          outbox_files: [{cell_key :: binary(), path :: binary()}]
        }

  @safe_chars ~r/^[A-Za-z0-9_.-]+$/

  @journal_cell_key "_task_journal"
  @inbox_cell_key "_task_inbox"

  # Reserved cell-keys of a workflow instance's task-journal and durable inbox,
  # each paired with the file it maps onto. They ride the Cell persistence
  # substrate — `wal_path/1` writes these names, so the
  # WalWriter/FsyncBatcher/delete machinery treats them as one more per-instance
  # file — but they are not Cells: `scan_dir/1` skips the files and the
  # WalWriter writes no dedup cache for the keys. One pairing serves both sides,
  # so a new reserved identity cannot land on a filename nothing skips.
  @reserved_file_by_key %{
    @journal_cell_key => "task.journal",
    @inbox_cell_key => "inbox.wal"
  }
  @reserved_cell_keys Map.keys(@reserved_file_by_key)
  @reserved_files Map.values(@reserved_file_by_key)

  @spec journal_cell_key() :: binary()
  def journal_cell_key(), do: @journal_cell_key

  @spec inbox_cell_key() :: binary()
  def inbox_cell_key(), do: @inbox_cell_key

  @doc """
  The reserved per-instance cell keys — identities that ride the Cell
  persistence substrate (WalWriter + FsyncBatcher) but are not Cells. The
  `WalWriter` skips its outbox dedup side-file for them, which is also what
  keeps `scan_dir/1` from ever surfacing one as a phantom Cell.
  """
  @spec reserved_cell_keys() :: [binary()]
  def reserved_cell_keys(), do: @reserved_cell_keys

  @doc """
  Reserved journal identity for a workflow instance's task-journal
  (`workflow-task-journal`). Workflow per-instance state is keyed by the
  instance id in the class position, so both dir levels carry it.
  """
  @spec journal_id(binary()) :: cell_id()
  def journal_id(instance_id) when is_binary(instance_id),
    do: {instance_id, instance_id, @journal_cell_key}

  @doc "On-disk path of a workflow instance's task-journal."
  @spec journal_path(binary()) :: binary()
  def journal_path(instance_id) when is_binary(instance_id),
    do: wal_path(journal_id(instance_id))

  @doc """
  Reserved inbox identity for a workflow instance's durable inbox
  (`workflow-durable-suspension`) — the sibling of `journal_id/1`.
  """
  @spec inbox_id(binary()) :: cell_id()
  def inbox_id(instance_id) when is_binary(instance_id),
    do: {instance_id, instance_id, @inbox_cell_key}

  @doc "On-disk path of a workflow instance's durable inbox."
  @spec inbox_path(binary()) :: binary()
  def inbox_path(instance_id) when is_binary(instance_id),
    do: wal_path(inbox_id(instance_id))

  @doc """
  This controller's state root, absolute.

  Resolved to an absolute path here, once, and nowhere else: a relative root
  names a different tree from every working directory, so the advisory lock,
  the refusal it raises, `Flezha.Boot.StateRootLock.held/0` and every WAL,
  snapshot and outbox path built from it would each be talking about something
  else. Expanding is not relocating: `app_data/state` still resolves against the
  working directory the controller was started from, which is why the boot says
  which directory that turned out to be.
  """
  @spec state_root() :: binary()
  def state_root(), do: Path.expand(Layout.dir(:state))

  # ----- namespace resolution (finish-platform-authn-authz) -----
  #
  # Durable state lives under `<state_root>/<namespace>/<operative_class>/`.
  # The wire's cell_id stays the 3-tuple: the namespace is resolved HERE,
  # from the class→namespace claim an Operative records when it binds, else
  # from the on-disk layout, else `_system`. One seam — every path in this
  # module (and every consumer of `class_dir/1`) inherits it.

  @claims_table :flezha_state_namespaces

  @doc """
  Create the class→namespace claims table. Called once by
  `Flezha.State.Supervisor` so the table lives with the state subsystem;
  every resolver below degrades to the on-disk layout when it is absent.
  """
  @spec ensure_claims_table() :: :ok
  def ensure_claims_table() do
    if :ets.whereis(@claims_table) == :undefined do
      :ets.new(@claims_table, [:named_table, :public, :set, read_concurrency: true])
    end

    :ok
  rescue
    # Lost the creation race — the table exists, which is all we need.
    ArgumentError -> :ok
  end

  @doc """
  Record that `operative_class`'s durable state belongs to `namespace`.

  A class already owned by ANOTHER namespace — live claim or durable files
  on disk — is refused: without this, starting a same-named Operative in a
  second namespace would silently write into (or read out of) the first
  one's state. The compatibility namespace `_system` is the exception: a
  class migrated there by `Flezha.Boot.StatePathMigrator` is ADOPTED by the
  first real namespace that claims it (one atomic rename), which is what
  keeps a pre-namespace deployment's operatives restarting seamlessly.
  """
  @spec claim_namespace(binary(), binary()) ::
          :ok | {:error, {:namespace_conflict, binary(), binary()}}
  def claim_namespace(operative_class, namespace)
      when is_binary(operative_class) and is_binary(namespace) do
    case current_owner(operative_class) do
      nil ->
        put_claim(operative_class, namespace)

      ^namespace ->
        put_claim(operative_class, namespace)

      "_system" ->
        adopt_from_system(operative_class, namespace)

      other ->
        {:error, {:namespace_conflict, operative_class, other}}
    end
  end

  @doc """
  The namespace owning `operative_class`'s durable state: the live claim,
  else the on-disk layout, else `_system`.
  """
  @spec namespace_of_class(binary()) :: binary()
  def namespace_of_class(operative_class) when is_binary(operative_class) do
    claimed_namespace(operative_class) || disk_namespace(operative_class) || "_system"
  end

  @doc "Directory holding one namespace's durable state — the first level under `state_root/0`."
  @spec namespace_dir(binary()) :: binary()
  def namespace_dir(namespace) when is_binary(namespace) do
    Path.join(state_root(), namespace)
  end

  @doc "Directory holding every durable file of one Operative class."
  @spec class_dir(binary()) :: binary()
  def class_dir(operative_class) when is_binary(operative_class) do
    Path.join(namespace_dir(namespace_of_class(operative_class)), operative_class)
  end

  @doc """
  Remove a class's state tree — the one guard every deletion of durable state
  passes: a tasklet directory holding a file under any in-progress suffix
  `Flezha.State.Durable` declares has a write in flight, and truncating it
  would corrupt the rename ceremony, so it is skipped (with WARN) and the class
  directory stays until it is empty.
  """
  @spec delete_class_dir(binary()) :: :ok
  def delete_class_dir(operative_class) when is_binary(operative_class) do
    class_dir = class_dir(operative_class)

    case File.ls(class_dir) do
      {:ok, names} ->
        Enum.each(names, fn name ->
          path = Path.join(class_dir, name)
          if File.dir?(path), do: maybe_delete_tasklet(path), else: File.rm(path)
        end)

        _ = File.rmdir(class_dir)
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp maybe_delete_tasklet(tasklet_dir) do
    case File.ls(tasklet_dir) do
      {:ok, names} ->
        if Enum.any?(names, &Durable.in_progress?/1) do
          Logger.warning(
            "[state] skipping state dir #{tasklet_dir}: write in flight " <>
              "(#{Enum.join(Durable.suffixes(), " / ")} present)"
          )
        else
          _ = File.rm_rf(tasklet_dir)
        end

      {:error, _} ->
        :ok
    end
  end

  # Claims are keyed by {state_root, class}: a claim is a fact about ONE
  # on-disk tree, and tests (as well as `FLEZHA_STATE_DIR` changes) repoint
  # the root — a stale claim from another tree must not leak into this one.
  defp claimed_namespace(operative_class) do
    case :ets.whereis(@claims_table) do
      :undefined ->
        nil

      _tid ->
        case :ets.lookup(@claims_table, {state_root(), operative_class}) do
          [{_, namespace}] -> namespace
          [] -> nil
        end
    end
  end

  defp put_claim(operative_class, namespace) do
    if :ets.whereis(@claims_table) != :undefined do
      :ets.insert(@claims_table, {{state_root(), operative_class}, namespace})
    end

    :ok
  end

  defp current_owner(operative_class) do
    claimed_namespace(operative_class) || disk_namespace(operative_class)
  end

  # The disk knows: the layout is `<root>/<namespace>/<class>/`, so the
  # namespace directory containing the class dir IS the owner.
  defp disk_namespace(operative_class) do
    Enum.find(namespaces(), &class_on_disk?(operative_class, &1))
  end

  @doc "Whether `operative_class` has a state directory under `namespace` on this node."
  @spec class_on_disk?(binary(), binary()) :: boolean()
  def class_on_disk?(operative_class, namespace)
      when is_binary(operative_class) and is_binary(namespace) do
    File.dir?(Path.join(namespace_dir(namespace), operative_class))
  end

  defp adopt_from_system(operative_class, namespace) do
    from = Path.join(namespace_dir("_system"), operative_class)
    to = Path.join(namespace_dir(namespace), operative_class)

    cond do
      not File.dir?(from) ->
        put_claim(operative_class, namespace)

      File.exists?(to) ->
        {:error, {:namespace_conflict, operative_class, "_system"}}

      true ->
        File.mkdir_p!(Path.dirname(to))

        case File.rename(from, to) do
          :ok ->
            put_claim(operative_class, namespace)

          {:error, _} ->
            # Lost an adoption race; re-resolve against the winner.
            case current_owner(operative_class) do
              ^namespace -> put_claim(operative_class, namespace)
              owner -> {:error, {:namespace_conflict, operative_class, owner || "_system"}}
            end
        end
    end
  end

  @spec state_dir(cell_id()) :: binary()
  def state_dir({operative_class, tasklet_id, _cell_key}) do
    Path.join(class_dir(operative_class), tasklet_id)
  end

  @spec wal_path(cell_id()) :: binary()
  def wal_path({_, _, cell_key} = pid) do
    name =
      Map.get_lazy(@reserved_file_by_key, cell_key, fn ->
        encode_cell_key(cell_key) <> ".wal"
      end)

    Path.join(state_dir(pid), name)
  end

  @spec snapshot_paths(cell_id()) :: [{non_neg_integer(), binary()}]
  def snapshot_paths({_, _, cell_key} = pid) do
    encoded = encode_cell_key(cell_key)
    prefix = encoded <> ".snap."

    pid
    |> state_dir()
    |> list_dir()
    |> Enum.flat_map(fn name ->
      case String.split(name, prefix, parts: 2) do
        ["", seq_str] ->
          case Integer.parse(seq_str) do
            {seq, ""} -> [{seq, Path.join(state_dir(pid), name)}]
            _ -> []
          end

        _ ->
          []
      end
    end)
    |> Enum.sort_by(fn {seq, _} -> seq end)
  end

  @spec outbox_dedup_path(cell_id()) :: binary()
  def outbox_dedup_path({_, _, cell_key} = pid) do
    Path.join(state_dir(pid), encode_cell_key(cell_key) <> ".outbox.dedup")
  end

  @doc """
  Path of the per-operative durable input-log (effectively-once tier).
  Lives at the class level beside the `kind` sidecar — inert to the
  Cell scanners, which only walk `<class>/<tasklet_id>/` dirs.
  """
  @spec input_log_path(binary()) :: binary()
  def input_log_path(operative_class) when is_binary(operative_class) do
    Path.join(class_dir(operative_class), "input.log")
  end

  @doc """
  Path of a per-Cell saga compensation marker (`workflow-operative-saga`
  §4.3). `:intent` marks that the Cell's tasklet completed forward and may
  need undo; `:done` marks that its compensation already ran. Presence is the
  signal (empty file). Inert to `scan_dir/1` (unknown suffix), so a marker never
  registers a phantom Cell; reclaimed with the operative-class dir on
  retention sweep.
  """
  @spec compensation_marker_path(cell_id(), :intent | :done) :: binary()
  def compensation_marker_path({_, _, cell_key} = pid, kind) when kind in [:intent, :done] do
    suffix = if kind == :intent, do: ".comp_intent", else: ".comp_done"
    Path.join(state_dir(pid), encode_cell_key(cell_key) <> suffix)
  end

  @doc """
  Write one compensation marker for a Cell (presence is the signal — empty
  body). Best-effort; returns the `File.write/2` result so the caller can log.
  """
  @spec write_compensation_marker(cell_id(), :intent | :done) :: :ok | {:error, term()}
  def write_compensation_marker({_, _, _} = pid, kind) when kind in [:intent, :done] do
    path = compensation_marker_path(pid, kind)
    File.mkdir_p(Path.dirname(path))
    File.write(path, "")
  end

  @doc """
  Read a Cell's saga compensation markers, shipped back on `RestoreInitState`
  so a respawned durable workflow driver finishes the remaining hooks: `intent` ⇒
  its tasklet completed forward and may need undo; `done` ⇒ its compensation
  already ran (skip it).
  """
  @spec compensation_markers(cell_id()) :: %{intent: boolean(), done: boolean()}
  def compensation_markers({_, _, _} = pid) do
    %{
      intent: File.exists?(compensation_marker_path(pid, :intent)),
      done: File.exists?(compensation_marker_path(pid, :done))
    }
  end

  @doc """
  Every on-disk path that belongs to one Cell (WAL, all snapshots, the
  outbox-dedup file, both compensation markers). Paths are returned whether or
  not the file exists — the one definition of "this Cell's durable set",
  shared by the hard-delete path and the cross-controller transfer.
  """
  @spec cell_files(cell_id()) :: [binary()]
  def cell_files({_, _, _} = pid) do
    [
      wal_path(pid),
      outbox_dedup_path(pid),
      compensation_marker_path(pid, :intent),
      compensation_marker_path(pid, :done)
    ] ++ Enum.map(snapshot_paths(pid), fn {_seq, path} -> path end)
  end

  @doc """
  Resolve one basename from `cell_files/1` back to its full path, or `nil`
  when the Cell's layout does not produce that name.

  The `nil` is a guard, not a convenience: the cross-controller transfer takes
  the name off the wire, and joining it onto `state_dir/1` would let a peer
  address any path on the node. Snapshots are rebuilt from the seq in the name
  rather than matched, since a node can legitimately be asked for a snapshot it
  does not hold yet.
  """
  @spec cell_file_path(cell_id(), binary()) :: binary() | nil
  def cell_file_path({_, _, cell_key} = pid, name) when is_binary(name) do
    Enum.find(cell_files(pid), &(Path.basename(&1) == name)) ||
      snapshot_file_path(pid, cell_key, name)
  end

  defp snapshot_file_path(pid, cell_key, name) do
    with ["", seq_str] <-
           String.split(name, encode_cell_key(cell_key) <> ".snap.", parts: 2),
         {seq, ""} <- Integer.parse(seq_str) do
      snapshot_path(pid, seq)
    else
      _ -> nil
    end
  end

  @doc """
  Cell ids under one Operative class — the per-Operative view every caller
  actually wants.

  Scans only this class's subtree. Filtering `all_cells/0` would make every
  caller pay for the whole state root, which turns any per-operative loop into
  a walk of every operative on the node.
  """
  @spec cells_of(binary()) :: [cell_id()]
  def cells_of(operative_class) when is_binary(operative_class) do
    for tasklet_id <- list_dir(class_dir(operative_class)),
        cell_key <- cell_keys_under({operative_class, tasklet_id, ""}),
        uniq: true,
        do: {operative_class, tasklet_id, cell_key}
  end

  @doc """
  Remove every on-disk file for one Cell (WAL, all snapshots, the
  outbox-dedup file). Idempotent: missing files are ignored. Used by the
  dynamic-mode hard-delete path (`Flezha.State.Supervisor.delete_cell/1`).
  """
  @spec delete_files(cell_id()) :: :ok
  def delete_files({operative_class, _, _} = pid) do
    pid
    |> cell_files()
    |> Enum.each(&File.rm/1)

    # Prune now-empty parent dirs so deleted state stops counting as durable
    # evidence of prior intent (intent-mismatch detection); rmdir fails
    # harmlessly while a sibling Cell or Tasklet still holds files.
    _ = File.rmdir(state_dir(pid))
    class_dir = class_dir(operative_class)
    _ = File.rmdir(class_dir)

    # A fully-reclaimed class releases its namespace claim, so the name can
    # be reused elsewhere without tripping the conflict refusal on a ghost.
    if not File.dir?(class_dir) and :ets.whereis(@claims_table) != :undefined do
      :ets.delete(@claims_table, {state_root(), operative_class})
    end

    :ok
  end

  @spec snapshot_path(cell_id(), non_neg_integer()) :: binary()
  def snapshot_path({_, _, cell_key} = pid, seq) do
    Path.join(
      state_dir(pid),
      encode_cell_key(cell_key) <> ".snap." <> Integer.to_string(seq)
    )
  end

  @doc """
  Enumerate Cell files under a Operative tree.

  Pass `{operative_class, tasklet_id, ""}` to scan the whole Tasklet. Returns lists
  of `{cell_key, ...}` tuples grouped by file type. A file wearing any
  in-progress suffix `Flezha.State.Durable` declares is skipped — it is
  crash-residue from an atomic-rename workflow, not a Cell.
  """
  @spec scan_dir(cell_id()) :: scan_result()
  def scan_dir({_, _, _} = pid) do
    dir = state_dir(pid)

    dir
    |> list_dir()
    |> Enum.reduce(%{wal_files: [], snapshot_files: [], outbox_files: []}, fn name, acc ->
      cond do
        Durable.in_progress?(name) ->
          acc

        # The per-instance journal and inbox ride the Cell substrate but are
        # not Cells; `inbox.wal` would otherwise decode as a Cell named
        # "inbox" and be shipped a `RestoreInitState`.
        name in @reserved_files ->
          acc

        String.ends_with?(name, ".wal") ->
          key = name |> String.trim_trailing(".wal") |> decode_cell_key()
          %{acc | wal_files: [{key, Path.join(dir, name)} | acc.wal_files]}

        String.ends_with?(name, ".outbox.dedup") ->
          key = name |> String.trim_trailing(".outbox.dedup") |> decode_cell_key()
          %{acc | outbox_files: [{key, Path.join(dir, name)} | acc.outbox_files]}

        true ->
          case parse_snapshot_name(name) do
            {:ok, encoded_key, seq} ->
              key = decode_cell_key(encoded_key)

              %{
                acc
                | snapshot_files: [{key, seq, Path.join(dir, name)} | acc.snapshot_files]
              }

            :error ->
              acc
          end
      end
    end)
  end

  @doc """
  Enumerate every Cell that has any persistence file on disk under
  `state_root()`. Walks the directory tree
  `<state_root>/<namespace>/<operative_class>/<tasklet_id>/`.
  """
  @spec all_cells() :: [cell_id()]
  def all_cells() do
    Enum.flat_map(operative_classes(), &cells_of/1)
  end

  @doc """
  Namespace directories under `state_root/` — the on-disk answer to "which
  namespaces hold durable state on this node", shared by the Watchdog's
  disk-quota sweep, the boot-time counter rebuild, and `operative_classes/0`.
  """
  @spec namespaces() :: [binary()]
  def namespaces() do
    state_root() |> list_dir() |> Enum.filter(&File.dir?(namespace_dir(&1)))
  end

  @doc """
  Operative classes holding durable state on this node — the on-disk answer to
  "which operatives are here at all", shared by the retention sweep, the remote
  durability tier's backstop, and `all_cells/0`.

  The tree is `<root>/<namespace>/<class>/`, so classes are the second level;
  class-level sidecars (`kind`, `input.log`, the declaration files) are regular
  files, so the directory filter is what separates a class from its own
  metadata.
  """
  @spec operative_classes() :: [binary()]
  def operative_classes() do
    for namespace <- namespaces(),
        dir = namespace_dir(namespace),
        class <- list_dir(dir),
        File.dir?(Path.join(dir, class)),
        uniq: true,
        do: class
  end

  @doc """
  Replace `path` atomically for the durable file `durable` declares: `write.(fd)`
  fills an in-progress sibling under the declared suffix, which is fsynced when
  the declaration says the install owes the sync, and then renamed over `path`.
  Any failure removes the sibling and leaves whatever was there before — never a
  half-written file, and never a residue a scanner would surface
  (`Flezha.State.Durable.in_progress?/1` recognises every declared suffix).

  The one definition of how a durable file lands. What varies between callers —
  the suffix, whether the ceremony fsyncs, whether it creates the directory — is
  read from `Flezha.State.Durable`'s row rather than re-implemented around it.
  """
  @spec atomic_install(binary(), (:file.io_device() -> :ok | {:error, term()}), atom()) ::
          :ok | {:error, term()}
  def atomic_install(path, write, durable) when is_function(write, 1) and is_atom(durable) do
    row = Durable.fetch!(durable)
    in_progress = Durable.in_progress_path(path, durable)

    with :ok <- ensure_dir(path, row.create_dir?),
         {:ok, fd} <- :file.open(in_progress, [:raw, :write, :binary]) do
      result = with :ok <- write.(fd), do: install_sync(fd, row.durability)
      _ = :file.close(fd)

      case result do
        :ok -> rename_or_discard(in_progress, path)
        {:error, _} = err -> discard_install(in_progress, err)
      end
    end
  end

  # A file whose owner already fsyncs on a cadence of its own does not pay a
  # second sync here; the declaration is what says which.
  defp install_sync(fd, :inline), do: :file.datasync(fd)
  defp install_sync(_fd, :batched), do: :ok

  # Where the directory's existence is the signal that a Cell is alive, the
  # install must fail with `:enoent` rather than resurrect a removed one.
  defp ensure_dir(path, true), do: File.mkdir_p(Path.dirname(path))
  defp ensure_dir(_path, false), do: :ok

  defp rename_or_discard(in_progress, path) do
    case File.rename(in_progress, path) do
      :ok -> :ok
      {:error, _} = err -> discard_install(in_progress, err)
    end
  end

  defp discard_install(in_progress, err) do
    _ = File.rm(in_progress)
    err
  end

  @doc """
  Snapshot files for a single Cell, sorted by seq ascending.
  """
  @spec snapshot_files(cell_id()) :: [{seq :: non_neg_integer(), path :: binary()}]
  def snapshot_files(pid), do: snapshot_paths(pid)

  @spec encode_cell_key(binary()) :: binary()
  def encode_cell_key(key) do
    if Regex.match?(@safe_chars, key) and not String.starts_with?(key, "_") do
      key
    else
      "_" <> Base.url_encode64(key, padding: false)
    end
  end

  @spec decode_cell_key(binary()) :: binary()
  def decode_cell_key("_" <> encoded) do
    case Base.url_decode64(encoded, padding: false) do
      {:ok, decoded} -> decoded
      :error -> "_" <> encoded
    end
  end

  def decode_cell_key(key), do: key

  defp parse_snapshot_name(name) do
    case Regex.run(~r/^(.+)\.snap\.(\d+)$/, name) do
      [_, encoded_key, seq_str] ->
        case Integer.parse(seq_str) do
          {seq, ""} -> {:ok, encoded_key, seq}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp cell_keys_under({_, _, _} = pid) do
    %{wal_files: w, snapshot_files: s, outbox_files: o} = scan_dir(pid)
    keys = Enum.map(w, &elem(&1, 0)) ++ Enum.map(s, &elem(&1, 0)) ++ Enum.map(o, &elem(&1, 0))
    Enum.uniq(keys)
  end

  defp list_dir(dir) do
    case File.ls(dir) do
      {:ok, names} -> names
      {:error, _} -> []
    end
  end
end
