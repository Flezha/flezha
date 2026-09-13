defmodule Flezha.Plugin.Provider.File do
  @moduledoc """
  A local file as an operative's input and as its output: `File.Source`
  (provider id `operative_file_source`) reads one line per entry frame, and
  `File.Sink` (`operative_file_sink`) writes one line per record.

  What both directions share lives here: the one folder every path must
  resolve inside, `FLEZHA_FILE_PLUGIN_ROOT` or its default, and the preparation that resolves a
  plugin's `path` against it. The contract, the options and the limits are
  `docs/plugins/file.md`.
  """

  alias Flezha.Config

  # Symbolic links followed while resolving one path, the bound Linux applies.
  @max_link_hops 40

  @doc """
  Resolve `path` inside `root`: expanded against the root, so a relative path
  names a file under it and an absolute path is taken as written, then walked
  component by component with every symbolic link followed.

  The file need not exist, but its parent directory must. The answer is the
  resolved absolute path, or a refusal naming the root the path leaves, or the
  directory that is missing.
  """
  @spec resolve_within_root(binary(), binary()) :: {:ok, binary()} | {:error, binary()}
  def resolve_within_root(path, root) do
    root = Path.expand(root)
    target = Path.expand(path, root)

    with {:ok, real_root} <- real_root(root),
         {:ok, resolved} <- resolve(target, path, root, real_root) do
      if inside?(resolved, real_root), do: {:ok, resolved}, else: refuse_outside(path, root)
    end
  end

  @doc """
  The preparation both providers share: the path resolved inside the root and
  written back as the one spelling every log line, event and verdict names, and
  the plugin id the plugin was enabled under when the options name none.
  """
  @spec prepare(map()) :: {:ok, map()} | {:error, binary()}
  def prepare(opts) do
    root = Config.get(:file_plugin_root)

    # Only the default folder is created. A configured one that is missing may be
    # a share that is not mounted, and writing beside it would go unnoticed.
    if root == Config.default(:file_plugin_root), do: File.mkdir_p(root)

    with {:ok, path} <- resolve_within_root(opts.path, root) do
      {:ok,
       Map.merge(opts, %{path: path, plugin_id: opts.plugin_id || opts[:source_plugin_id] || path})}
    end
  end

  defp real_root(root) do
    case walk(root) do
      {:ok, real} -> if File.dir?(real), do: {:ok, real}, else: refuse_root(root)
      _missing_or_unreadable -> refuse_root(root)
    end
  end

  defp resolve(target, path, root, real_root) do
    case walk(target) do
      {:ok, resolved} ->
        {:ok, resolved}

      {:missing, parent, [name]} ->
        {:ok, Path.join(parent, name)}

      {:missing, _deepest, _rest} ->
        refuse_missing_parent(target, path, root, real_root)

      {:error, :too_many_links} ->
        {:error, "file plugin path #{path} crosses more than #{@max_link_hops} symbolic links"}

      {:error, reason} ->
        {:error, "file plugin path #{path} cannot be resolved: #{inspect(reason)}"}
    end
  end

  # From `/`, one component at a time: a link is replaced by its target and the
  # walk starts again from the top, so a link anywhere in the path is judged by
  # where it lands. The kernel follows an intermediate link silently, which is
  # why no single `read_link_all/1` of the whole path can see one.
  defp walk(absolute), do: walk("/", tl(Path.split(absolute)), @max_link_hops)

  defp walk(resolved, [], _hops), do: {:ok, resolved}

  defp walk(resolved, [name | rest] = remaining, hops) do
    candidate = Path.join(resolved, name)

    case :file.read_link_all(candidate) do
      {:error, :einval} ->
        walk(candidate, rest, hops)

      {:ok, _link} when hops == 0 ->
        {:error, :too_many_links}

      {:ok, link} ->
        [_root | components] =
          link |> IO.chardata_to_string() |> Path.expand(resolved) |> Path.split()

        walk("/", components ++ rest, hops - 1)

      {:error, :enoent} ->
        {:missing, resolved, remaining}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp inside?(_path, "/"), do: true
  defp inside?(path, root), do: String.starts_with?(path, root <> "/")

  defp refuse_root(root),
    do: {:error, "the file plugin root folder #{root} does not exist or is not a directory"}

  defp refuse_outside(path, root),
    do: {:error, "file plugin path #{path} resolves outside the root folder #{root}"}

  # A directory outside the root is not named: whether it exists is nothing an
  # enable is entitled to learn.
  defp refuse_missing_parent(target, path, root, real_root) do
    if inside?(target, root) or inside?(target, real_root),
      do:
        {:error, "file plugin path #{path}: the directory #{Path.dirname(target)} does not exist"},
      else: refuse_outside(path, root)
  end

  defmodule Source do
    @moduledoc """
    A file read onto an entry channel (`:io_binding`), one line per frame,
    followed as it grows.

    Each line is injected through the operative's confirming ingest, and the
    byte offset where the last delivered line ended is committed through the
    plugin's persisted option `offset`, so a plugin restart, a move to another
    controller and a controller restart all resume after the last committed
    line. Delivery is at-least-once: a crash between a delivery and its commit
    re-delivers, never skips.

    Required: `path` and an entry channel, written `channel` or `channel_in`;
    the rest: `docs/plugins/file.md`.
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.File, as: FilePlugin
    alias Flezha.Plugin.Provider.File.ReadWorker
    alias Flezha.Plugin.ProviderBehaviour

    use ProviderBehaviour,
      name: "file_source",
      feeds: true,
      options: [
        path: [
          type: :string,
          required: true,
          doc: "The file to read: relative to FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it"
        ],
        channel: [
          type: :names,
          doc:
            "The channel this plugin is attached to: the entry channel every line is injected on, and one name. Required, unless `channel_in` names it"
        ],
        channel_in: [
          type: :names,
          doc:
            "Entry channel every line is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
        ],
        plugin_id: [
          type: :string,
          doc: "Plugin id; defaults to the id the plugin is enabled under"
        ],
        start_at: [
          type: :enum,
          values: ["beginning", "end"],
          default: "beginning",
          doc:
            "Where a reader with no committed offset starts: byte 0, or the size of the file when it is first opened"
        ],
        poll_interval_ms: [
          type: :int,
          default: 1_000,
          min: 1,
          doc: "Milliseconds between looks for new bytes"
        ],
        max_line_bytes: [
          type: :int,
          default: 1_048_576,
          min: 1,
          doc: "Longest line accepted; a longer one stops the reader at the offset it starts at"
        ],
        read_chunk_bytes: [
          type: :int,
          default: 65_536,
          min: 1,
          doc:
            "Bytes read at a time, and never more than one read ahead of the last delivered line"
        ],
        commit_interval_ms: [
          type: :int,
          default: 1_000,
          min: 1,
          doc:
            "Longest a delivered line's offset waits to be committed while the reader has not caught up; what a crash can re-deliver"
        ],
        failure_threshold: [
          type: :int,
          default: 3,
          min: 1,
          doc: "Consecutive failed reads or refused ingests before the plugin reports unhealthy"
        ],
        idle_after_ms: [
          type: :int,
          default: 0,
          min: 0,
          doc:
            "Age of the last settled poll past which the reader is idle; 0 derives it from `poll_interval_ms`"
        ],
        # The committed offset, written back by the reader and read on every
        # restart path, rather than an option an operator writes.
        offset: [type: :int, min: 0, internal: true]
      ],
      worker: [module: ReadWorker, restart: :temporary]

    # A channel written as an atom, which an Elixir-side spec may carry, handed
    # back to the row as the string its type converts.
    defp convert_value(field, value) when field in [:channel, :channel_in] and is_atom(value),
      do: convert_value(field, Atom.to_string(value))

    @doc """
    Validate and resolve. Reads nothing: the worker opens the file at
    activation. The root walk is a bounded run of `lstat`s on the local disk,
    done here so a refusal reaches the operator who typed the path.
    """
    @impl true
    def init_provider(opts) do
      with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
           do: FilePlugin.prepare(opts)
    end

    @doc """
    Teardown is the host's. This callback exists so the provider is
    subscribable at all: every subscription requires the matching export.
    """
    @impl true
    def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

    @doc """
    Health verdict from the reader: a stop names the offset an operator must
    look at, a missing file names the path, a failure run reaching
    `failure_threshold` names its error, and a reader whose last settled poll is
    older than the idle window says how old. A file that does not grow is
    healthy.
    """
    @impl true
    @spec health(map()) :: ProviderBehaviour.health_verdict()
    def health(opts) do
      ProviderBehaviour.worker_health(
        opts,
        # The outer budget exceeds the probe's own, so this provider's precise
        # `:probe_timeout` is not replaced by the generic budget verdict.
        [
          absent: :no_reader,
          dead: :reader_dead,
          budget_ms: 2 * ProviderBehaviour.probe_budget_ms()
        ],
        &reader_health(&1, opts)
      )
    end

    @doc "What `flz operative info` shows beneath the plugin: the path and its verdict."
    @impl true
    def inspect_state(opts), do: %{path: opts.path, health: health(opts)}

    defp reader_health(pid, opts) do
      case ReadWorker.probe(pid) do
        {:ok, facts} -> verdict(facts, opts)
        :timeout -> {:fail_open, :probe_timeout}
      end
    end

    defp verdict(%{stopped: {_reason, _offset} = stop}, _opts), do: {:unhealthy, stop}
    defp verdict(%{missing: true, path: path}, _opts), do: {:unhealthy, {:file_missing, path}}

    defp verdict(%{consecutive_failures: failures, last_error: error}, opts)
         when failures >= opts.failure_threshold,
         do: {:unhealthy, {:read_failing, error}}

    defp verdict(%{last_settled_at: settled_at}, opts),
      do: ProviderBehaviour.idle_verdict(settled_at, opts.idle_after_ms, opts.poll_interval_ms)
  end

  defmodule Sink do
    @moduledoc """
    Records written to a file (`:io_binding`), one line per record: the
    message bytes and a newline, nothing else.

    Records are batched, written in one call and made durable with a data sync
    before any outbox hold in the batch is confirmed, so nothing is lost. What
    the file is, `commit` names. Under `append` it is the path, grown in place,
    and a batch synced just before a crash, whose confirmation did not reach
    the outbox, is appended a second time after it. Under `rename` the path is
    a directory of immutable parts, and a record the outbox guarantees is
    written once.

    Required: `path`; the rest: `docs/plugins/file.md`.
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.File, as: FilePlugin
    alias Flezha.Plugin.Provider.File.WriteWorker
    alias Flezha.Plugin.Provider.WorkerSup
    alias Flezha.Plugin.ProviderBehaviour
    alias Flezha.State.Outbox

    use ProviderBehaviour,
      name: "file_sink",
      options: [
        path: [
          type: :string,
          required: true,
          doc:
            "The file to append to, or under `commit: rename` the directory parts are committed into: relative to FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it"
        ],
        channel_out: [
          type: :names,
          doc:
            "Write only records whose metadata channel matches. Several names, comma-separated, write them all"
        ],
        plugin_id: [
          type: :string,
          doc: "Plugin id; defaults to the id the plugin is enabled under"
        ],
        commit: [
          type: :enum,
          values: ["append", "rename"],
          default: "append",
          doc:
            "The file layout. `append` grows one file in place, and a batch synced just before a crash is appended again after it; `rename` commits immutable parts into a directory, and writes a record the outbox guarantees once"
        ],
        roll_after_ms: [
          type: :int,
          default: 10_000,
          min: 1,
          doc:
            "Under `rename`, the longest a part stays open, and so the longest a guaranteed record waits for its confirmation"
        ],
        roll_bytes: [
          type: :int,
          default: 67_108_864,
          min: 1,
          doc: "Under `rename`, the size at which a part is committed before `roll_after_ms`"
        ],
        linger_ms: [type: :int, default: 25, min: 1, doc: "How long a batch waits to fill"],
        max_batch: [type: :int, default: 500, min: 1, doc: "Records in one write"],
        queue_max: [
          type: :int,
          default: 10_000,
          min: 1,
          doc: "Records held before enqueue blocks"
        ],
        retry_backoff_ms: [
          type: :int,
          default: 250,
          min: 1,
          doc: "First delay after a failed write"
        ],
        retry_max_backoff_ms: [type: :int, default: 5_000, min: 1, doc: "Ceiling for that delay"],
        unhealthy_after_ms: [
          type: :int,
          default: 30_000,
          min: 1,
          doc: "Age of the last durable write past which a backed-up sink is unhealthy"
        ]
      ],
      worker: [module: WriteWorker, restart: :temporary]

    @doc """
    Validate and resolve the path. Opens nothing: the worker does, at
    activation. Under `rename` the path is the directory the parts go into,
    made here with mode `0700` when it does not exist, so a path that names
    anything else is refused to the operator who typed it.
    """
    @impl true
    def init_provider(opts) do
      with {:ok, opts} <- FilePlugin.prepare(opts), do: prepare_layout(opts)
    end

    defp prepare_layout(%{commit: "rename", path: dir} = opts) do
      case File.lstat(dir) do
        {:ok, %File.Stat{type: :directory}} ->
          {:ok, opts}

        {:ok, %File.Stat{}} ->
          {:error,
           "file plugin path #{dir} is not a directory, and commit: rename writes parts into one"}

        {:error, :enoent} ->
          with :ok <- File.mkdir(dir), :ok <- File.chmod(dir, 0o700) do
            {:ok, opts}
          else
            {:error, reason} ->
              {:error,
               "file plugin path #{dir}: the directory cannot be made: #{inspect(reason)}"}
          end

        {:error, reason} ->
          {:error, "file plugin path #{dir} cannot be read: #{inspect(reason)}"}
      end
    end

    defp prepare_layout(opts), do: {:ok, opts}

    @impl true
    def on_input(%Ctx.IO{} = ctx, opts), do: write(ctx, opts)
    @impl true
    def on_output(%Ctx.IO{} = ctx, opts), do: write(ctx, opts)

    # A delivery-guaranteed record is held before it is handed over, and the
    # worker answers the hold once the record is durable: synced under
    # `append`, in a committed part under `rename`.
    defp write(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
      if ProviderBehaviour.in_channel?(md, opts[:filter_channel]) do
        Outbox.hold(md)
        :ok = WriteWorker.enqueue(WorkerSup.worker(__MODULE__, opts.plugin_id), msg, md)
      end

      {:ok, ctx}
    end

    @doc """
    Health verdict (plugin health contract): unhealthy with no worker or a dead
    one, when the file cannot be opened or, under `rename`, the directory cannot
    be reconciled, and when records are pending behind a failing write and no
    batch has been synced within `unhealthy_after_ms`.
    """
    @impl true
    @spec health(map()) :: ProviderBehaviour.health_verdict()
    def health(opts) do
      ProviderBehaviour.worker_health(
        opts,
        [absent: :no_sink, dead: :worker_dead],
        &writer_health(&1, opts.unhealthy_after_ms)
      )
    end

    @doc "What `flz operative info` shows beneath the plugin: the path and its verdict."
    @impl true
    def inspect_state(opts), do: %{path: opts.path, health: health(opts)}

    defp writer_health(pid, threshold) do
      case WriteWorker.stats(pid) do
        %{open_error: error} when error != nil ->
          {:unhealthy, {:open_failed, error}}

        %{pending: pending, consecutive_failures: failures, last_flush_age_ms: age} = stats
        when pending > 0 and failures > 0 and age > threshold ->
          {:unhealthy, {:write_stalled, stats.last_error}}

        %{} ->
          :healthy
      end
    end
  end
end
