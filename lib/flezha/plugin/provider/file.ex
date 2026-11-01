defmodule Flezha.Plugin.Provider.File do
  @moduledoc """
  A file as a relay's input and as its output: `File.Source` (provider id
  `relay_file_input`) reads one line per entry frame, and `File.Sink`
  (`relay_file_output`) writes one line per record.

  What both directions share lives here: the preparation that resolves a
  plugin's `path` inside its namespace's own folder under
  `FLEZHA_FILE_PLUGIN_ROOT`, a folder or an `s3://` bucket prefix, whose rules
  are its storage's (`File.Storage`). The contract, the options and the limits
  are `docs/public/plugins/file.md`.
  """

  alias Flezha.Plugin.Provider.File.Storage

  @doc """
  The preparation both providers share: the path resolved inside the root and
  written back as the one spelling every log line, event and verdict names, and
  the plugin id the plugin was enabled under when the options name none.
  """
  @spec prepare(map()) :: {:ok, map()} | {:error, term()}
  def prepare(opts) do
    with {:ok, path} <- Storage.Config.resolve_path(opts.path, opts.source_namespace) do
      {:ok,
       Map.merge(opts, %{path: path, plugin_id: opts.plugin_id || opts[:source_plugin_id] || path})}
    end
  end

  defmodule Source do
    @moduledoc """
    A file, or an S3 object, read onto an entry channel (a relay plugin), one
    line per frame, followed as it grows.

    Each line is injected through the relay's confirming ingest, and the
    byte offset where the last delivered line ended is committed through the
    plugin's persisted option `offset`, so a plugin restart, a move to another
    controller and a controller restart all resume after the last committed
    line. Delivery is at-least-once: a crash between a delivery and its commit
    re-delivers, never skips.

    Required: `path` and an entry channel, written `channel` or `channel_in`;
    the rest: `docs/public/plugins/file.md`.
    """

    alias Flezha.Plugin.Provider.File, as: FilePlugin
    alias Flezha.Plugin.Provider.File.ReadWorker
    alias Flezha.Plugin.ProviderBehaviour
    alias Flezha.Plugin.ProviderBehaviour.Options

    use ProviderBehaviour,
      name: "file_source",
      description: "The plugin that reads lines from a file into a relay.",
      emits: [shipped: nil, failed: nil],
      telemetry: [
        truncated:
          {:event,
           label: "File restarted",
           meaning: "The file was cut short or replaced, so it is read again from the start.",
           sentence: "{plugin_id} reads {path} again from the start"}
      ],
      source: [partitioned: :own_copy, idle_interval: :poll_interval_ms],
      options:
        [
          path: [
            type: :string,
            required: true,
            doc:
              "The file to read: relative to the namespace's folder under FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it. Under an s3:// root, an object under the namespace's prefix"
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
          # The committed offset, written back by the reader and read on every
          # restart path, rather than an option a user writes.
          offset: [type: :int, min: 0, internal: true]
        ] ++
          Options.channel(:in, "line") ++
          Options.plugin_id("the id it was enabled under") ++
          Options.failure_threshold() ++ Options.idle_after(),
      worker: [module: ReadWorker, restart: :temporary]

    @doc """
    Validate and resolve. Reads nothing: the worker opens the file at
    activation. The root check is a steps run of `lstat`s under a folder root
    and makes no request under an S3 one, done here so a refusal reaches the
    user who typed the path.
    """
    @impl true
    def init_provider(opts), do: FilePlugin.prepare(opts)

    @doc "What `flz relay info` shows beneath the plugin: the path and its verdict."
    @impl true
    def inspect_state(opts), do: %{path: opts.path, health: health(opts)}

    # Before the source's shared verdicts: a stop names the offset a user must
    # look at and a missing file names the path. A file that does not grow is
    # healthy.
    defp verdict(%{stopped: {_reason, _offset} = stop}, _opts), do: {:unhealthy, stop}
    defp verdict(%{missing: true, path: path}, _opts), do: {:unhealthy, {:file_missing, path}}
  end

  defmodule Sink do
    @moduledoc """
    Records written to a file (a relay plugin), one line per record: the
    message bytes and a newline, nothing else.

    Records are batched, written in one call and made durable with a data sync
    before any outbox hold in the batch is confirmed, so nothing is lost. What
    the file is, `commit` names. Under `append` it is the path, grown in place,
    and a batch synced just before a crash, whose confirmation did not reach
    the outbox, is appended a second time after it. Under `rename` the path is
    a directory of immutable parts, and a record the outbox guarantees is
    written once. Under an S3 root `path` is a key prefix, `rename` is the only
    layout, and the open roll is held in memory.

    Required: `path`; the rest: `docs/public/plugins/file.md`.
    """

    alias Flezha.Plugin.Extension.Ctx
    alias Flezha.Plugin.Provider.File, as: FilePlugin
    alias Flezha.Plugin.Provider.File.Storage
    alias Flezha.Plugin.Provider.File.WriteWorker
    alias Flezha.Plugin.Provider.WorkerSup
    alias Flezha.Plugin.ProviderBehaviour
    alias Flezha.Plugin.ProviderBehaviour.Options
    alias Flezha.State.Outbox

    use ProviderBehaviour,
      name: "file_sink",
      description: "The plugin that writes a relay's output to a file.",
      emits: [shipped: nil, failed: nil],
      telemetry: [
        refused:
          {:event,
           label: "Record refused",
           outcome: :failure,
           meaning: "A record was refused because it cannot be written as one line.",
           sentence: "{plugin_id} refused a record for {path}"}
      ],
      options:
        [
          path: [
            type: :string,
            required: true,
            doc:
              "The file to append to, or under `commit: rename` the directory parts are committed into: relative to the namespace's folder under FLEZHA_FILE_PLUGIN_ROOT, or absolute inside it. Under an s3:// root, the key prefix parts are committed under, inside the namespace's prefix"
          ],
          commit: [
            type: :enum,
            values: ["append", "rename"],
            default: "append",
            doc:
              "The file layout. `append` grows one file in place, and a batch synced just before a crash is appended again after it; `rename` commits immutable parts into a directory, and writes a record the outbox guarantees once. Under an s3:// root only `rename`"
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
            doc:
              "Under `rename`, the size at which a part is committed before `roll_after_ms`. Under an s3:// root the open part is held in memory, and at most 5368709120"
          ],
          linger_ms: [type: :int, default: 25, min: 1, doc: "How long a batch waits to fill"],
          max_batch: [type: :int, default: 500, min: 1, doc: "Records in one write"],
          unhealthy_after_ms: [
            type: :int,
            default: 30_000,
            min: 1,
            doc: "Age of the last durable write past which a backed-up output plugin is unhealthy"
          ]
        ] ++
          Options.channel(:out, "record") ++
          Options.plugin_id("the id it was enabled under") ++
          Options.queue([:queue_max]) ++ Options.retry(),
      worker: [module: WriteWorker, restart: :temporary]

    @doc """
    Validate and resolve the path. Opens nothing: the worker does, at
    activation. What the layout needs first, a directory made or a layout the
    storage cannot carry refused, is the storage's
    (`File.Storage.prepare_sink/1`), done here so a refusal reaches the user
    who typed it.
    """
    @impl true
    def init_provider(opts) do
      with {:ok, opts} <- FilePlugin.prepare(opts),
           do: Storage.Config.of(opts.path).prepare_sink(opts)
    end

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
        :ok = WriteWorker.enqueue(WorkerSup.worker(__MODULE__, opts.worker_key), msg, md)
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
        [absent: :no_worker, dead: :worker_dead],
        &writer_health(&1, opts.unhealthy_after_ms)
      )
    end

    @doc "What `flz relay info` shows beneath the plugin: the path and its verdict."
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
