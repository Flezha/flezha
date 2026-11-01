defmodule Flezha.State.Durable do
  @moduledoc """
  Every file this node keeps durable bytes in, declared once.

  A row says three things about one file: how it is **framed** — `:record` for a
  log appended and cut record by record under `Flezha.State.RecordFrame`,
  `:whole` for a file replaced entire, whatever its bytes hold — the
  **durability** its writer relies on (`:inline` when the install fsyncs before
  it renames, `:inline_dir` when it also fsyncs the directory after, `:batched`
  when a sync the owner already performs makes the bytes durable), and the
  **in-progress suffix** the file wears while it is being written.

  Two things derive from the table and nothing else. `Paths.atomic_install/3`
  reads a row for the suffix it writes under and the sync it owes, so a new
  durable file is a row rather than a fourth copy of the ceremony. Every scanner
  that must ignore a file being written asks `in_progress?/1`, so a new suffix
  cannot leave one scanner surfacing crash residue as a Cell.

  `file` is the source that owns the bytes. It is a path and not a module
  because the table sits in `state/` and two of its owners are ranked above it
  (`scripts/check-layer-direction.sh`): naming their modules here would be an
  upward reference, and the test that keeps the table and the tree in step
  reads the path.
  """

  @typedoc """
  How a durable file is written: appended and cut record by record, or replaced
  entire. A transferred WAL holds records and is `:whole` — it is installed as
  one file, never cut in place.
  """
  @type frame :: :record | :whole

  @typedoc """
  What makes the bytes durable: the install's own fsync (and the directory's,
  `:inline_dir`), or a sync the owner already runs on a cadence of its own.
  """
  @type durability :: :inline | :inline_dir | :batched

  @type row :: %{
          file: binary(),
          frame: frame(),
          durability: durability(),
          in_progress_suffix: binary(),
          unique?: boolean(),
          create_dir?: boolean()
        }

  # One row per durable file, in the order a restore meets them:
  #
  #   id | the source that owns the bytes | frame | durability | suffix | unique? | create_dir?
  #
  # `unique?` files carry a per-write integer after the suffix: two writers may
  # race the same key there, and a shared in-progress path would let one
  # truncate the other's bytes and the loser rename them into place.
  # `create_dir?` is false where the directory's existence is the signal that
  # the Cell is alive: an install that made the directory would resurrect one
  # a teardown had removed.
  #
  # The outbox dedup file is `:batched` and not `:inline` on purpose: the WAL is
  # the truth and a missing dedup file reads as the defaults, so the writer's
  # own fsync batch is what makes this file's watermark mean anything, and an
  # install fsync would double the syncs per batch.
  @columns [:file, :frame, :durability, :in_progress_suffix, :unique?, :create_dir?]

  @table [
    {:wal, "lib/flezha/state/wal_writer.ex", :record, :batched, ".rewriting", false, true},
    {:input_log, "lib/flezha/state/input_log/server.ex", :record, :batched, ".rewriting", false,
     true},
    {:effects_log, "lib/flezha/state/input_log/server.ex", :record, :inline, ".rewriting", false,
     true},
    {:task_journal, "lib/flezha/state/task_journal.ex", :record, :batched, ".rewriting", false,
     true},
    {:snapshot, "lib/flezha/state/snapshot.ex", :whole, :inline, ".tmp", false, true},
    {:outbox_dedup, "lib/flezha/state/lineage.ex", :whole, :batched, ".tmp", false, false},
    {:lineage, "lib/flezha/state/lineage.ex", :whole, :inline_dir, ".tmp", false, false},
    {:completion, "lib/flezha/state/completion.ex", :whole, :inline, ".tmp", false, true},
    {:class_kind, "lib/flezha/relay/class_registry.ex", :whole, :inline, ".tmp", false, true},
    {:seat, "lib/flezha/relay/class_registry.ex", :whole, :inline_dir, ".tmp", false, true},
    {:dlq_entries, "lib/flezha/dlq/store.ex", :whole, :inline, ".tmp", false, true},
    {:log_index, "lib/flezha/logs/index.ex", :whole, :inline, ".tmp", false, true},
    {:tier_object, "lib/flezha/state/durability_tier/backend/local_path.ex", :whole, :inline,
     ".tmp", true, true},
    {:tier_artifact, "lib/flezha/state/durability_tier/artifact.ex", :whole, :inline,
     ".transferring", false, true},
    {:wal_transfer, "lib/flezha/state/wal_transfer.ex", :whole, :inline, ".transferring", false,
     true},
    {:file_sink_progress, "lib/flezha/plugin/provider/file/storage/local.ex", :whole, :inline,
     ".tmp", false, false},
    {:landed_mark, "lib/flezha/state/adoption.ex", :whole, :inline, ".tmp", false, true}
  ]

  @rows Map.new(@table, fn row ->
          [id | values] = Tuple.to_list(row)
          {id, Map.new(Enum.zip(@columns, values))}
        end)

  @suffixes @rows
            |> Map.values()
            |> Enum.map(& &1.in_progress_suffix)
            |> Enum.uniq()
            |> Enum.sort()

  @doc "Every declared durable file, keyed by its id."
  @spec all() :: %{atom() => row()}
  def all, do: @rows

  @doc "The declaration for `id`; raises when nothing declares it."
  @spec fetch!(atom()) :: row()
  def fetch!(id), do: Map.fetch!(@rows, id)

  @doc "The declared in-progress suffixes, sorted."
  @spec suffixes() :: [binary()]
  def suffixes, do: @suffixes

  @record_suffixes @rows
                   |> Map.values()
                   |> Enum.filter(&(&1.frame == :record))
                   |> Enum.map(& &1.in_progress_suffix)
                   |> Enum.uniq()

  # Every record log is cut by the one rewrite in `Flezha.State.RecordFrame`, so
  # they cannot declare two suffixes for it.
  if length(@record_suffixes) != 1 do
    raise "the record logs declare #{length(@record_suffixes)} rewrite suffixes: " <>
            inspect(@record_suffixes)
  end

  @doc """
  The suffix a record log's rewrite fills before it renames into place.

  One suffix for every log on the record frame: the cut is one function and it
  cannot wear a different name per log.
  """
  @spec record_rewrite_suffix() :: binary()
  def record_rewrite_suffix, do: hd(@record_suffixes)

  @doc """
  The path `id`'s writer fills before it renames onto `path`.

  A unique-suffixed file gets a per-write integer, so two writers racing one
  key cannot share an in-progress path.
  """
  @spec in_progress_path(binary(), atom()) :: binary()
  def in_progress_path(path, id) do
    case fetch!(id) do
      %{in_progress_suffix: suffix, unique?: true} ->
        path <> suffix <> "." <> Integer.to_string(System.unique_integer([:positive]))

      %{in_progress_suffix: suffix} ->
        path <> suffix
    end
  end

  @doc """
  Whether `name` is a file being written rather than one to read.

  The one exclusion rule every directory scanner asks, so a durable file added
  to the table can never be surfaced as a phantom Cell by a scanner that was
  not edited.

  A `.claim` sidecar is not in-progress and is not declared here: it is the
  fencing counter a durability-tier key carries beside its object
  (`remote-durability-tier`), it outlives every write, and a scanner excludes
  it by name where it lives.
  """
  @spec in_progress?(binary()) :: boolean()
  def in_progress?(name) when is_binary(name) do
    Enum.any?(@suffixes, fn suffix ->
      String.ends_with?(name, suffix) or String.contains?(name, suffix <> ".")
    end)
  end
end
