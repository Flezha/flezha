defmodule Flezha.State.Paths do
  require Logger

  @moduledoc """
  Path resolution for per-Cell WAL, snapshot, outbox-dedup and lineage files.

  All persistence files live under
  `<state_root>/<namespace>/<relay_class>/<tasklet_id>/`, where
  `<state_root>` is the `:flezha, :state_root` application env (set by tests
  for isolation), else `storage/app/state`. The user-facing knob is the
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
  alias Flezha.State.{Durable, Ref}

  @type cell_id ::
          {relay_class :: binary(), tasklet_id :: binary(), cell_key :: binary()}

  @type scan_result :: %{
          wal_files: [{cell_key :: binary(), path :: binary()}],
          snapshot_files: [
            {cell_key :: binary(), seq :: non_neg_integer(), path :: binary()}
          ],
          side_files: [{cell_key :: binary(), path :: binary()}]
        }

  @safe_chars ~r/^[A-Za-z0-9_.-]+$/

  @journal_cell_key "_task_journal"

  # The reserved cell-key of a steps instance's task-journal, paired with the
  # file it maps onto. It rides the Cell persistence substrate — `wal_path/1`
  # writes this name, so the WalWriter/FsyncBatcher/delete machinery treats it
  # as one more per-instance file — but it is not a Cell: `scan_dir/1` skips the
  # file and the WalWriter writes no dedup cache for the key. One pairing serves
  # both sides, so a new reserved identity cannot land on a filename nothing
  # skips.
  @reserved_file_by_key %{@journal_cell_key => "task.journal"}
  @reserved_cell_keys Map.keys(@reserved_file_by_key)
  @reserved_files Map.values(@reserved_file_by_key)

  @spec journal_cell_key() :: binary()
  def journal_cell_key(), do: @journal_cell_key

  @doc """
  The reserved per-instance cell keys — identities that ride the Cell
  persistence substrate (WalWriter + FsyncBatcher) but are not Cells. The
  `WalWriter` skips its outbox dedup side-file for them, and `scan_dir/1` never
  surfaces one as a phantom Cell.
  """
  @spec reserved_cell_keys() :: [binary()]
  def reserved_cell_keys(), do: @reserved_cell_keys

  @doc """
  Reserved journal identity for a steps instance's task-journal
  (`steps-task-journal`). Steps relay per-instance state is keyed by the
  instance id in the class position, so both dir levels carry it.
  """
  @spec journal_id(binary()) :: cell_id()
  def journal_id(instance_id) when is_binary(instance_id),
    do: {instance_id, instance_id, @journal_cell_key}

  @doc "On-disk path of a steps instance's task-journal."
  @spec journal_path(binary()) :: binary()
  def journal_path(instance_id) when is_binary(instance_id),
    do: wal_path(journal_id(instance_id))

  @doc """
  This controller's state root, absolute.

  Resolved to an absolute path here, once, and nowhere else: a relative root
  names a different tree from every working directory, so the advisory lock,
  the refusal it raises, `Flezha.Boot.StateRootLock.held/0` and every WAL,
  snapshot and outbox path built from it would each be talking about something
  else. Expanding is not relocating: `storage/app/state` still resolves against the
  working directory the controller was started from, which is why the boot says
  which directory that turned out to be.
  """
  @spec state_root() :: binary()
  def state_root() do
    root = Layout.dir(:state)
    if Path.type(root) == :absolute, do: Path.expand(root), else: expanded_relative(root)
  end

  # Expanding a relative root asks the file server for the working directory on
  # every path built from it. The controller never changes its working directory
  # after boot, so each configured value is expanded once.
  defp expanded_relative(root) do
    key = {__MODULE__, :state_root, root}

    with nil <- :persistent_term.get(key, nil) do
      expanded = Path.expand(root)
      :persistent_term.put(key, expanded)
      expanded
    end
  end

  # ----- namespace resolution (finish-platform-authn-authz) -----
  #
  # Durable state lives under `<state_root>/<namespace>/<relay_class>/`.
  # The wire's cell_id stays the 3-tuple: the namespace is resolved HERE,
  # from the class→namespace claim a Relay records when it binds, else
  # from the on-disk layout, else `_system`. One seam — every path in this
  # module (and every consumer of `class_dir/1`) inherits it.

  @claims_table :flezha_state_namespaces

  # The siblings a class directory is set aside under while a newer copy of its
  # state is fetched, discarded under once that copy is in place, and the record
  # of its landing (`Flezha.State.Adoption`); no scanner reads one as a class.
  @aside %{set_aside: ".superseded", discarded: ".discarded", landed: ".landed"}

  @doc """
  Create the class→namespace claims table. Created at application boot
  (`Flezha.State.Supervisor.ensure_tables/0`) so it outlives every writer;
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
  Record that `relay_class`'s durable state belongs to `namespace`.

  A class already owned by ANOTHER namespace — live claim or durable files
  on disk — is refused: without this, starting a same-named Relay in a
  second namespace would silently write into (or read out of) the first
  one's state. The compatibility namespace `_system` is the exception: a
  class migrated there by `Flezha.Boot.StatePathMigrator` is ADOPTED by the
  first real namespace that claims it (one atomic rename), which is what
  keeps a pre-namespace deployment's relays restarting seamlessly.
  """
  @spec claim_namespace(binary(), binary()) ::
          :ok | {:error, {:namespace_conflict, binary(), binary()}}
  def claim_namespace(relay_class, namespace)
      when is_binary(relay_class) and is_binary(namespace) do
    case current_owner(relay_class) do
      nil ->
        put_claim(relay_class, namespace)

      ^namespace ->
        put_claim(relay_class, namespace)

      "_system" ->
        adopt_from_system(relay_class, namespace)

      other ->
        {:error, {:namespace_conflict, relay_class, other}}
    end
  end

  @doc """
  The namespace owning `relay_class`'s durable state: the live claim,
  else the on-disk layout, else `_system`.
  """
  @spec namespace_of_class(binary()) :: binary()
  def namespace_of_class(relay_class) when is_binary(relay_class) do
    held_namespace(relay_class) || "_system"
  end

  @doc "Directory holding one namespace's durable state — the first level under `state_root/0`."
  @spec namespace_dir(binary()) :: binary()
  def namespace_dir(namespace) when is_binary(namespace) do
    Path.join(state_root(), namespace)
  end

  @doc "Directory holding every durable file of one Relay class."
  @spec class_dir(binary()) :: binary()
  def class_dir(relay_class) when is_binary(relay_class) do
    Path.join(namespace_dir(namespace_of_class(relay_class)), relay_class)
  end

  @doc """
  Remove a class's state tree — the one guard every deletion of durable state
  passes: a tasklet directory holding a file under any in-progress suffix
  `Flezha.State.Durable` declares has a write in flight, and truncating it
  would corrupt the rename ceremony, so it is skipped (with WARN) and the class
  directory, with its namespace claim, stays until it is empty.
  """
  @spec delete_class_dir(binary()) :: :ok
  def delete_class_dir(relay_class) when is_binary(relay_class) do
    class_dir = class_dir(relay_class)

    with {:ok, names} <- File.ls(class_dir) do
      Enum.each(names, fn name ->
        path = Path.join(class_dir, name)
        if File.dir?(path), do: maybe_delete_tasklet(path), else: File.rm(path)
      end)
    end

    prune_class_dir(relay_class)
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
  defp claimed_namespace(relay_class) do
    case :ets.whereis(@claims_table) do
      :undefined ->
        nil

      _tid ->
        case :ets.lookup(@claims_table, {state_root(), relay_class}) do
          [{_, namespace}] -> namespace
          [] -> nil
        end
    end
  end

  defp put_claim(relay_class, namespace) do
    if :ets.whereis(@claims_table) != :undefined do
      :ets.insert(@claims_table, {{state_root(), relay_class}, namespace})
    end

    :ok
  end

  defp current_owner(relay_class) do
    claimed_namespace(relay_class) || disk_namespace(relay_class)
  end

  # The disk knows: the layout is `<root>/<namespace>/<class>/`, so the
  # namespace directory containing the class dir IS the owner.
  defp disk_namespace(relay_class) do
    Enum.find(namespaces(), &class_on_disk?(relay_class, &1))
  end

  @doc """
  Whether this node's state root holds `relay_class`: a namespace claims it, or
  its directory is on disk. The local half of the cluster lookup of where an
  instance's state is.
  """
  @spec holds_class?(binary()) :: boolean()
  def holds_class?(relay_class) when is_binary(relay_class),
    do: held_namespace(relay_class) != nil

  defp held_namespace(relay_class),
    do: claimed_namespace(relay_class) || disk_namespace(relay_class)

  @doc "Whether `relay_class` has a state directory under `namespace` on this node."
  @spec class_on_disk?(binary(), binary()) :: boolean()
  def class_on_disk?(relay_class, namespace)
      when is_binary(relay_class) and is_binary(namespace) do
    File.dir?(Path.join(namespace_dir(namespace), relay_class))
  end

  defp adopt_from_system(relay_class, namespace) do
    from = Path.join(namespace_dir("_system"), relay_class)
    to = Path.join(namespace_dir(namespace), relay_class)

    cond do
      not File.dir?(from) ->
        put_claim(relay_class, namespace)

      File.exists?(to) ->
        {:error, {:namespace_conflict, relay_class, "_system"}}

      true ->
        File.mkdir_p!(Path.dirname(to))

        case File.rename(from, to) do
          :ok ->
            put_claim(relay_class, namespace)

          {:error, _} ->
            # Lost an adoption race; re-resolve against the winner.
            case current_owner(relay_class) do
              ^namespace -> put_claim(relay_class, namespace)
              owner -> {:error, {:namespace_conflict, relay_class, owner || "_system"}}
            end
        end
    end
  end

  @spec state_dir(cell_id()) :: binary()
  def state_dir({relay_class, tasklet_id, _cell_key}) do
    Path.join(class_dir(relay_class), tasklet_id)
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
    dir = state_dir(pid)
    snaps = for name <- list_dir(dir), seq = snapshot_seq(cell_key, name), do: {seq, name}

    snaps
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {seq, name} -> {seq, Path.join(dir, name)} end)
  end

  defp snapshot_seq(cell_key, name) do
    with ["", seq_str] <- String.split(name, encode_cell_key(cell_key) <> ".snap.", parts: 2),
         {seq, ""} <- Integer.parse(seq_str),
         do: seq,
         else: (_ -> nil)
  end

  @spec outbox_dedup_path(cell_id()) :: binary()
  def outbox_dedup_path(pid), do: cell_file(pid, ".outbox.dedup")

  @spec lineage_path(cell_id()) :: binary()
  def lineage_path(pid), do: cell_file(pid, ".lineage")

  defp cell_file({_, _, cell_key} = pid, suffix),
    do: Path.join(state_dir(pid), encode_cell_key(cell_key) <> suffix)

  @doc """
  Path of an instance's durable input log (a flow relay on the effectively-once
  tier, or a durable steps relay). Lives at the class level beside the `kind`
  sidecar — inert to the Cell scanners, which only walk `<class>/<tasklet_id>/`
  dirs.
  """
  @spec input_log_path(binary()) :: binary()
  def input_log_path(relay_class) when is_binary(relay_class),
    do: input_log_in(class_dir(relay_class))

  @doc "`input_log_path/1` inside a class directory the caller already found."
  @spec input_log_in(binary()) :: binary()
  def input_log_in(class_dir), do: Path.join(class_dir, "input.log")

  @doc """
  Path of an instance's effect log beside its input log: the results its
  tasklets' `ctx.effect` calls kept, retained as long as their inputs.
  """
  @spec effects_log_path(binary()) :: binary()
  def effects_log_path(relay_class) when is_binary(relay_class) do
    Path.join(class_dir(relay_class), "effects.log")
  end

  @doc """
  Path of a per-Cell saga compensation marker (`relay-saga`
  §4.3). `:intent` marks that the Cell's tasklet completed forward and may
  need undo; `:done` marks that its compensation already ran. Presence is the
  signal (empty file). Inert to `scan_dir/1` (unknown suffix), so a marker never
  registers a phantom Cell; reclaimed with the relay-class dir on
  retention sweep.
  """
  @spec compensation_marker_path(cell_id(), :intent | :done) :: binary()
  def compensation_marker_path(pid, :intent), do: cell_file(pid, ".comp_intent")
  def compensation_marker_path(pid, :done), do: cell_file(pid, ".comp_done")

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
  so a respawned durable steps driver finishes the remaining hooks: `intent` ⇒
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
  outbox-dedup and lineage files, both compensation markers), whether or not
  the file exists: the one definition of "this Cell's durable set", shared by
  the hard-delete path and the cross-controller transfer (`Flezha.State.Ref.files/1`).
  """
  @spec cell_files(cell_id()) :: [binary()]
  def cell_files({_, _, _} = pid) do
    [
      wal_path(pid),
      outbox_dedup_path(pid),
      lineage_path(pid),
      compensation_marker_path(pid, :intent),
      compensation_marker_path(pid, :done)
    ] ++ Enum.map(snapshot_paths(pid), fn {_seq, path} -> path end)
  end

  @typedoc """
  A unit of an instance's durable state that moves between nodes whole: one
  Cell's file set (a steps relay's journal is this shape, under its reserved
  identity) or the instance's input log.
  """
  @type durable_ref :: {:cell, cell_id()} | {:input_log, binary()} | {:effects_log, binary()}

  @doc """
  The durable references this node holds for `relay_class`: its Cells, a
  steps relay's journal and the input log, each only when present.
  """
  @spec durable_refs(binary()) :: [durable_ref()]
  def durable_refs(relay_class) when is_binary(relay_class) do
    journal =
      if File.exists?(journal_path(relay_class)), do: [journal_id(relay_class)], else: []

    logs =
      for {kind, path} <- [
            input_log: input_log_path(relay_class),
            effects_log: effects_log_path(relay_class)
          ],
          File.exists?(path),
          do: {kind, relay_class}

    for(cell <- Enum.uniq(cells_of(relay_class) ++ journal), do: {:cell, cell}) ++ logs
  end

  @doc """
  Remove `ref`'s files (`Flezha.State.Ref.files/1`; missing ones are ignored),
  pruning the directories they leave empty: the hard delete of a Cell
  (`Flezha.State.Supervisor.delete_cell/1`) and the name a peer calls over RPC
  (`WalTransfer.move_cell/3`).
  """
  @spec delete_durable(durable_ref()) :: :ok
  def delete_durable(ref) do
    files = Ref.files(ref)
    Enum.each(files, &File.rm/1)
    # Deleted state stops counting as durable evidence of prior intent
    # (intent-mismatch detection); rmdir fails harmlessly while a sibling Cell
    # or Tasklet still holds files.
    files |> Enum.map(&Path.dirname/1) |> Enum.uniq() |> Enum.each(&File.rmdir/1)
    prune_class_dir(Ref.state_key(ref))
  end

  @doc """
  Where `relay_class`'s directory is set aside (`:set_aside`) or discarded
  (`:discarded`) while a newer copy replaces it, and where its landing is
  recorded (`:landed`).
  """
  @spec aside_path(binary(), :set_aside | :discarded | :landed) :: binary()
  def aside_path(relay_class, which), do: class_dir(relay_class) <> Map.fetch!(@aside, which)

  @doc "The path of `pid`'s snapshot file `name`, `nil` when `name` is no snapshot's."
  @spec snapshot_file_path(cell_id(), binary()) :: binary() | nil
  def snapshot_file_path({_, _, cell_key} = pid, name) do
    if seq = snapshot_seq(cell_key, name), do: snapshot_path(pid, seq)
  end

  @doc """
  Cell ids under one Relay class — the per-Relay view every caller
  actually wants.

  Scans only this class's subtree. Filtering `all_cells/0` would make every
  caller pay for the whole state root, which turns any per-relay loop into
  a walk of every relay on the node.
  """
  @spec cells_of(binary()) :: [cell_id()]
  def cells_of(relay_class) when is_binary(relay_class),
    do: cells_in(relay_class, class_dir(relay_class))

  defp cells_in(relay_class, class_dir) do
    for tasklet_id <- list_dir(class_dir),
        cell_key <- cell_keys_in(Path.join(class_dir, tasklet_id)),
        uniq: true,
        do: {relay_class, tasklet_id, cell_key}
  end

  defp prune_class_dir(relay_class) do
    class_dir = class_dir(relay_class)
    _ = File.rmdir(class_dir)

    # A fully-reclaimed class releases its namespace claim, so the name can
    # be reused elsewhere without tripping the conflict refusal on a ghost.
    if not File.dir?(class_dir) and :ets.whereis(@claims_table) != :undefined do
      :ets.delete(@claims_table, {state_root(), relay_class})
    end

    :ok
  end

  @spec snapshot_path(cell_id(), non_neg_integer()) :: binary()
  def snapshot_path(pid, seq), do: cell_file(pid, ".snap." <> Integer.to_string(seq))

  @doc """
  Enumerate Cell files under a Relay tree.

  Pass `{relay_class, tasklet_id, ""}` to scan the whole Tasklet. Returns lists
  of `{cell_key, ...}` tuples grouped by file type. A file wearing any
  in-progress suffix `Flezha.State.Durable` declares is skipped — it is
  crash-residue from an atomic-rename steps relay, not a Cell.
  """
  @spec scan_dir(cell_id()) :: scan_result()
  def scan_dir({_, _, _} = pid), do: scan(state_dir(pid))

  defp scan(dir) do
    dir
    |> list_dir()
    |> Enum.reduce(%{wal_files: [], snapshot_files: [], side_files: []}, fn name, acc ->
      cond do
        Durable.in_progress?(name) ->
          acc

        # The per-instance journal rides the Cell substrate but is not a
        # Cell, so it is never shipped a `RestoreInitState`.
        name in @reserved_files ->
          acc

        String.ends_with?(name, ".wal") ->
          key = name |> String.trim_trailing(".wal") |> decode_cell_key()
          %{acc | wal_files: [{key, Path.join(dir, name)} | acc.wal_files]}

        suffix = Enum.find([".outbox.dedup", ".lineage"], &String.ends_with?(name, &1)) ->
          key = name |> String.trim_trailing(suffix) |> decode_cell_key()

          # A hydration writes the dedup cache of every identity it restores,
          # the journal's included, which is still no Cell.
          if key in @reserved_cell_keys,
            do: acc,
            else: %{acc | side_files: [{key, Path.join(dir, name)} | acc.side_files]}

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
  `<state_root>/<namespace>/<relay_class>/<tasklet_id>/`.
  """
  @spec all_cells() :: [cell_id()]
  def all_cells() do
    Enum.flat_map(class_dirs(), fn {relay_class, dir} -> cells_in(relay_class, dir) end)
  end

  @doc """
  Namespace directories under `state_root/` — the on-disk answer to "which
  namespaces hold durable state on this node", shared by the Watchdog's
  disk-quota sweep, the boot-time counter rebuild, and `relay_classes/0`.
  """
  @spec namespaces() :: [binary()]
  def namespaces() do
    state_root() |> list_dir() |> Enum.filter(&File.dir?(namespace_dir(&1)))
  end

  @doc """
  Relay classes holding durable state on this node — the on-disk answer to
  "which relays are here at all", shared by the retention sweep, the remote
  durability tier's ship sweep, and `all_cells/0`.

  The tree is `<root>/<namespace>/<class>/`, so classes are the second level;
  class-level sidecars (`kind`, `input.log`, the declaration files) are regular
  files, so the directory filter is what separates a class from its own
  metadata.
  """
  @spec relay_classes() :: [binary()]
  def relay_classes(), do: Enum.map(class_dirs(), &elem(&1, 0))

  @doc """
  `relay_classes/0` with the directory each class was found in. A walk that
  goes on into the classes starts from these, so it never resolves a class's
  namespace again.
  """
  @spec class_dirs() :: [{binary(), binary()}]
  def class_dirs() do
    found =
      for namespace <- namespaces(),
          dir = namespace_dir(namespace),
          class <- list_dir(dir),
          class_dir = Path.join(dir, class),
          File.dir?(class_dir),
          not String.ends_with?(class, [@aside.set_aside, @aside.discarded]),
          do: {class, class_dir}

    Enum.uniq_by(found, &elem(&1, 0))
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
        :ok -> rename_or_discard(in_progress, path, row.durability)
        {:error, _} = err -> discard_install(in_progress, err)
      end
    end
  end

  # A file whose owner already fsyncs on a cadence of its own does not pay a
  # second sync here; the declaration is what says which.
  defp install_sync(_fd, :batched), do: :ok
  defp install_sync(fd, _inline), do: :file.datasync(fd)

  # A renamed-in file survives a power loss only once its directory entry does.
  defp sync_dir(path, :inline_dir) do
    with {:ok, synced} <- File.open(Path.dirname(path), [:read, :raw, :directory], &:file.sync/1),
         do: synced
  end

  defp sync_dir(_path, _durability), do: :ok

  # Where the directory's existence is the signal that a Cell is alive, the
  # install must fail with `:enoent` rather than resurrect a removed one.
  defp ensure_dir(path, true), do: File.mkdir_p(Path.dirname(path))
  defp ensure_dir(_path, false), do: :ok

  defp rename_or_discard(in_progress, path, durability) do
    case File.rename(in_progress, path) do
      :ok -> sync_dir(path, durability)
      {:error, _} = err -> discard_install(in_progress, err)
    end
  end

  defp discard_install(in_progress, err) do
    _ = File.rm(in_progress)
    err
  end

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

  defp cell_keys_in(dir) do
    %{wal_files: w, snapshot_files: s, side_files: o} = scan(dir)
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
