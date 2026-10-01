defmodule Flezha.State.Paths do
  require Logger

  @moduledoc """
  Path resolution for per-Cell WAL, snapshot, and outbox-dedup files.

  All persistence files live under
  `<state_root>/<namespace>/<operator_class>/<tasklet_id>/`, where
  `<state_root>` is the `:flezha, :state_root` application env (set by tests
  for isolation), else `app_data/state`. The user-facing knob is the
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
  alias Flezha.State.{Durable, ScopePaths}

  @type cell_id ::
          {operator_class :: binary(), tasklet_id :: binary(), cell_key :: binary()}

  @type scan_result :: %{
          wal_files: [{cell_key :: binary(), path :: binary()}],
          snapshot_files: [
            {cell_key :: binary(), seq :: non_neg_integer(), path :: binary()}
          ],
          outbox_files: [{cell_key :: binary(), path :: binary()}]
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
  (`steps-task-journal`). Steps operator per-instance state is keyed by the
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
  else. Expanding is not relocating: `app_data/state` still resolves against the
  working directory the controller was started from, which is why the boot says
  which directory that turned out to be.
  """
  @spec state_root() :: binary()
  def state_root(), do: Path.expand(Layout.dir(:state))

  # ----- namespace resolution (finish-platform-authn-authz) -----
  #
  # Durable state lives under `<state_root>/<namespace>/<operator_class>/`.
  # The wire's cell_id stays the 3-tuple: the namespace is resolved HERE,
  # from the class→namespace claim an Operator records when it binds, else
  # from the on-disk layout, else `_system`. One seam — every path in this
  # module (and every consumer of `class_dir/1`) inherits it.

  @claims_table :flezha_state_namespaces

  # The siblings of a class directory that `Flezha.State.Adoption` stages a
  # newer copy in and sets the old one aside in.
  @staged ".adopting"
  @set_aside ".superseded"

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
  Record that `operator_class`'s durable state belongs to `namespace`.

  A class already owned by ANOTHER namespace — live claim or durable files
  on disk — is refused: without this, starting a same-named Operator in a
  second namespace would silently write into (or read out of) the first
  one's state. The compatibility namespace `_system` is the exception: a
  class migrated there by `Flezha.Boot.StatePathMigrator` is ADOPTED by the
  first real namespace that claims it (one atomic rename), which is what
  keeps a pre-namespace deployment's operators restarting seamlessly.
  """
  @spec claim_namespace(binary(), binary()) ::
          :ok | {:error, {:namespace_conflict, binary(), binary()}}
  def claim_namespace(operator_class, namespace)
      when is_binary(operator_class) and is_binary(namespace) do
    case current_owner(operator_class) do
      nil ->
        put_claim(operator_class, namespace)

      ^namespace ->
        put_claim(operator_class, namespace)

      "_system" ->
        adopt_from_system(operator_class, namespace)

      other ->
        {:error, {:namespace_conflict, operator_class, other}}
    end
  end

  @doc """
  The namespace owning `operator_class`'s durable state: the live claim,
  else the on-disk layout, else `_system`.
  """
  @spec namespace_of_class(binary()) :: binary()
  def namespace_of_class(operator_class) when is_binary(operator_class) do
    class = adopting_for(operator_class) || operator_class
    claimed_namespace(class) || disk_namespace(class) || "_system"
  end

  @doc "Directory holding one namespace's durable state — the first level under `state_root/0`."
  @spec namespace_dir(binary()) :: binary()
  def namespace_dir(namespace) when is_binary(namespace) do
    Path.join(state_root(), namespace)
  end

  @doc "Directory holding every durable file of one Operator class."
  @spec class_dir(binary()) :: binary()
  def class_dir(operator_class) when is_binary(operator_class) do
    Path.join(namespace_dir(namespace_of_class(operator_class)), operator_class)
  end

  @doc """
  Remove a class's state tree — the one guard every deletion of durable state
  passes: a tasklet directory holding a file under any in-progress suffix
  `Flezha.State.Durable` declares has a write in flight, and truncating it
  would corrupt the rename ceremony, so it is skipped (with WARN) and the class
  directory stays until it is empty.
  """
  @spec delete_class_dir(binary()) :: :ok
  def delete_class_dir(operator_class) when is_binary(operator_class) do
    class_dir = class_dir(operator_class)

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
  defp claimed_namespace(operator_class) do
    case :ets.whereis(@claims_table) do
      :undefined ->
        nil

      _tid ->
        case :ets.lookup(@claims_table, {state_root(), operator_class}) do
          [{_, namespace}] -> namespace
          [] -> nil
        end
    end
  end

  defp put_claim(operator_class, namespace) do
    if :ets.whereis(@claims_table) != :undefined do
      :ets.insert(@claims_table, {{state_root(), operator_class}, namespace})
    end

    :ok
  end

  defp current_owner(operator_class) do
    claimed_namespace(operator_class) || disk_namespace(operator_class)
  end

  # The disk knows: the layout is `<root>/<namespace>/<class>/`, so the
  # namespace directory containing the class dir IS the owner.
  defp disk_namespace(operator_class) do
    Enum.find(namespaces(), &class_on_disk?(operator_class, &1))
  end

  @doc "Whether `operator_class` has a state directory under `namespace` on this node."
  @spec class_on_disk?(binary(), binary()) :: boolean()
  def class_on_disk?(operator_class, namespace)
      when is_binary(operator_class) and is_binary(namespace) do
    File.dir?(Path.join(namespace_dir(namespace), operator_class))
  end

  defp adopt_from_system(operator_class, namespace) do
    from = Path.join(namespace_dir("_system"), operator_class)
    to = Path.join(namespace_dir(namespace), operator_class)

    cond do
      not File.dir?(from) ->
        put_claim(operator_class, namespace)

      File.exists?(to) ->
        {:error, {:namespace_conflict, operator_class, "_system"}}

      true ->
        File.mkdir_p!(Path.dirname(to))

        case File.rename(from, to) do
          :ok ->
            put_claim(operator_class, namespace)

          {:error, _} ->
            # Lost an adoption race; re-resolve against the winner.
            case current_owner(operator_class) do
              ^namespace -> put_claim(operator_class, namespace)
              owner -> {:error, {:namespace_conflict, operator_class, owner || "_system"}}
            end
        end
    end
  end

  @spec state_dir(cell_id()) :: binary()
  def state_dir({operator_class, tasklet_id, _cell_key}) do
    Path.join(class_dir(operator_class), tasklet_id)
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
  Path of an instance's durable input log (a flow operator on the effectively-once
  tier, or a durable steps operator). Lives at the class level beside the `kind`
  sidecar — inert to the Cell scanners, which only walk `<class>/<tasklet_id>/`
  dirs.
  """
  @spec input_log_path(binary()) :: binary()
  def input_log_path(operator_class) when is_binary(operator_class) do
    Path.join(class_dir(operator_class), "input.log")
  end

  @doc """
  Path of a per-Cell saga compensation marker (`operator-saga`
  §4.3). `:intent` marks that the Cell's tasklet completed forward and may
  need undo; `:done` marks that its compensation already ran. Presence is the
  signal (empty file). Inert to `scan_dir/1` (unknown suffix), so a marker never
  registers a phantom Cell; reclaimed with the operator-class dir on
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
  outbox-dedup file, both compensation markers). Paths are returned whether or
  not the file exists — the one definition of "this Cell's durable set",
  shared by the hard-delete path and the cross-controller transfer
  (`durable_files/1`).
  """
  @spec cell_files(cell_id()) :: [binary()]
  def cell_files({_, _, _} = pid) do
    [
      wal_path(pid),
      outbox_dedup_path(pid),
      compensation_marker_path(pid, :intent),
      compensation_marker_path(pid, :done)
    ] ++ Enum.map(snapshot_paths(pid), fn {_seq, path} -> path end) ++ scope_facts(pid)
  end

  # A stepped scope's facts ride its journal, so whatever moves the journal
  # moves them.
  defp scope_facts({_, _, @journal_cell_key} = journal), do: [scope_fact_path(journal)]
  defp scope_facts(_pid), do: []

  @doc """
  Where a stepped scope keeps what outlives its outer instance's process
  while it is off the runner (`Flezha.Operator.Scopes`), beside its journal
  `journal`.
  """
  @spec scope_fact_path(cell_id()) :: binary()
  def scope_fact_path({_, _, @journal_cell_key} = journal),
    do: Path.join(state_dir(journal), "scope.fact")

  @typedoc """
  A unit of an instance's durable state that moves between nodes whole: one
  Cell's file set (a steps operator's journal is this shape, under its reserved
  identity) or the instance's input log.
  """
  @type durable_ref :: {:cell, cell_id()} | {:input_log, binary()}

  @doc """
  The durable references this node holds for `operator_class`: its Cells, a
  steps operator's journal and the input log, each only when present.
  """
  @spec durable_refs(binary()) :: [durable_ref()]
  def durable_refs(operator_class) when is_binary(operator_class) do
    # A staged copy keeps an instance's journal under the instance's own id.
    journal_cell =
      put_elem(journal_id(adopting_for(operator_class) || operator_class), 0, operator_class)

    journal = if File.exists?(wal_path(journal_cell)), do: [journal_cell], else: []

    log =
      if File.exists?(input_log_path(operator_class)),
        do: [{:input_log, operator_class}],
        else: []

    # An instance's stepped scopes live under its directory and travel with it.
    scopes =
      for scope_id <- ScopePaths.scope_ids(operator_class), ref <- durable_refs(scope_id), do: ref

    for(cell <- Enum.uniq(cells_of(operator_class) ++ journal), do: {:cell, cell}) ++
      log ++ scopes
  end

  @doc "Every path `ref` is made of, whether or not the file exists."
  @spec durable_files(durable_ref()) :: [binary()]
  def durable_files({:cell, cell_id}), do: cell_files(cell_id)
  def durable_files({:input_log, operator_class}), do: [input_log_path(operator_class)]

  @doc """
  Resolve one basename of `ref` back to its full path, or `nil` when the
  reference's layout does not produce that name.

  The `nil` is a guard, not a convenience: the cross-controller transfer takes
  the name off the wire, and joining it onto a directory would let a peer
  address any path on the node. Snapshots are rebuilt from the seq in the name
  rather than matched, since a node can legitimately be asked for a snapshot it
  does not hold yet.
  """
  @spec durable_file_path(durable_ref(), binary()) :: binary() | nil
  def durable_file_path({:cell, {_, _, cell_key} = pid} = ref, name) when is_binary(name) do
    named(ref, name) || snapshot_file_path(pid, cell_key, name)
  end

  def durable_file_path({:input_log, _operator_class} = ref, name) when is_binary(name),
    do: named(ref, name)

  defp named(ref, name), do: Enum.find(durable_files(ref), &(Path.basename(&1) == name))

  @doc "Remove `ref`'s files, pruning the directories they leave empty."
  @spec delete_durable(durable_ref()) :: :ok
  def delete_durable({:cell, cell_id}), do: delete_files(cell_id)

  def delete_durable({:input_log, operator_class} = ref) do
    ref |> durable_files() |> Enum.each(&File.rm/1)
    prune_class_dir(operator_class)
  end

  @doc "The class a newer copy of `operator_class` is staged under, in its namespace."
  @spec staging_class(binary()) :: binary()
  def staging_class(operator_class), do: operator_class <> @staged

  @doc "The class `operator_class`'s directory is set aside under while a staged copy replaces it."
  @spec set_aside_class(binary()) :: binary()
  def set_aside_class(operator_class), do: operator_class <> @set_aside

  @doc """
  The class a staged or set-aside directory belongs to, or `nil` for a class
  directory. Neither sibling is a class to any scanner. A stepped scope under
  one belongs to the scope of the same name under the class.
  """
  @spec adopting_for(binary()) :: binary() | nil
  def adopting_for(name) do
    outer = ScopePaths.outer_of(name)

    Enum.find_value([@staged, @set_aside], fn suffix ->
      if String.ends_with?(outer, suffix),
        do: String.replace_suffix(outer, suffix, "") <> ScopePaths.suffix(name)
    end)
  end

  @doc """
  Remove a staged or set-aside directory, the one way an adoption deletes
  state. A path that is no such sibling is left alone.
  """
  @spec remove_adoption_dir(binary()) :: :ok
  def remove_adoption_dir(path) do
    if adopting_for(Path.basename(path)), do: File.rm_rf(path)
    :ok
  end

  @doc "`ref` as the same reference under `operator_class`, a stepped scope's under its scope there."
  @spec ref_under(durable_ref(), binary()) :: durable_ref()
  def ref_under({:cell, {class, tasklet_id, cell_key}}, operator_class),
    do: {:cell, {operator_class <> ScopePaths.suffix(class), tasklet_id, cell_key}}

  def ref_under({:input_log, class}, operator_class),
    do: {:input_log, operator_class <> ScopePaths.suffix(class)}

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
  Cell ids under one Operator class — the per-Operator view every caller
  actually wants.

  Scans only this class's subtree. Filtering `all_cells/0` would make every
  caller pay for the whole state root, which turns any per-operator loop into
  a walk of every operator on the node.
  """
  @spec cells_of(binary()) :: [cell_id()]
  def cells_of(operator_class) when is_binary(operator_class) do
    for tasklet_id <- list_dir(class_dir(operator_class)),
        cell_key <- cell_keys_under({operator_class, tasklet_id, ""}),
        uniq: true,
        do: {operator_class, tasklet_id, cell_key}
  end

  @doc """
  Remove every on-disk file for one Cell (WAL, all snapshots, the
  outbox-dedup file). Idempotent: missing files are ignored. Used by the
  dynamic-mode hard-delete path (`Flezha.State.Supervisor.delete_cell/1`).
  """
  @spec delete_files(cell_id()) :: :ok
  def delete_files({operator_class, _, _} = pid) do
    pid
    |> cell_files()
    |> Enum.each(&File.rm/1)

    # Prune now-empty parent dirs so deleted state stops counting as durable
    # evidence of prior intent (intent-mismatch detection); rmdir fails
    # harmlessly while a sibling Cell or Tasklet still holds files.
    _ = File.rmdir(state_dir(pid))
    prune_class_dir(operator_class)
  end

  defp prune_class_dir(operator_class) do
    class_dir = class_dir(operator_class)
    _ = File.rmdir(class_dir)

    # A fully-reclaimed class releases its namespace claim, so the name can
    # be reused elsewhere without tripping the conflict refusal on a ghost.
    if not File.dir?(class_dir) and :ets.whereis(@claims_table) != :undefined do
      :ets.delete(@claims_table, {state_root(), operator_class})
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
  Enumerate Cell files under an Operator tree.

  Pass `{operator_class, tasklet_id, ""}` to scan the whole Tasklet. Returns lists
  of `{cell_key, ...}` tuples grouped by file type. A file wearing any
  in-progress suffix `Flezha.State.Durable` declares is skipped — it is
  crash-residue from an atomic-rename steps operator, not a Cell.
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

        # The per-instance journal rides the Cell substrate but is not a
        # Cell, so it is never shipped a `RestoreInitState`.
        name in @reserved_files ->
          acc

        String.ends_with?(name, ".wal") ->
          key = name |> String.trim_trailing(".wal") |> decode_cell_key()
          %{acc | wal_files: [{key, Path.join(dir, name)} | acc.wal_files]}

        String.ends_with?(name, ".outbox.dedup") ->
          key = name |> String.trim_trailing(".outbox.dedup") |> decode_cell_key()

          # A hydration writes the dedup cache of every identity it restores,
          # the journal's included, which is still no Cell.
          if key in @reserved_cell_keys,
            do: acc,
            else: %{acc | outbox_files: [{key, Path.join(dir, name)} | acc.outbox_files]}

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
  `<state_root>/<namespace>/<operator_class>/<tasklet_id>/`.
  """
  @spec all_cells() :: [cell_id()]
  def all_cells() do
    Enum.flat_map(operator_classes(), &cells_of/1)
  end

  @doc """
  Namespace directories under `state_root/` — the on-disk answer to "which
  namespaces hold durable state on this node", shared by the Watchdog's
  disk-quota sweep, the boot-time counter rebuild, and `operator_classes/0`.
  """
  @spec namespaces() :: [binary()]
  def namespaces() do
    state_root() |> list_dir() |> Enum.filter(&File.dir?(namespace_dir(&1)))
  end

  @doc """
  Operator classes holding durable state on this node — the on-disk answer to
  "which operators are here at all", shared by the retention sweep, the remote
  durability tier's backstop, and `all_cells/0`.

  The tree is `<root>/<namespace>/<class>/`, so classes are the second level;
  class-level sidecars (`kind`, `input.log`, the declaration files) are regular
  files, so the directory filter is what separates a class from its own
  metadata.
  """
  @spec operator_classes() :: [binary()]
  def operator_classes() do
    for namespace <- namespaces(),
        dir = namespace_dir(namespace),
        class <- list_dir(dir),
        File.dir?(Path.join(dir, class)),
        adopting_for(class) == nil,
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

  @doc "The entries of `dir`, or none when it cannot be listed."
  @spec list_dir(binary()) :: [binary()]
  def list_dir(dir) do
    case File.ls(dir) do
      {:ok, names} -> names
      {:error, _} -> []
    end
  end
end
