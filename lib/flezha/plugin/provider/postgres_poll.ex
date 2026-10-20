defmodule Flezha.Plugin.Provider.PostgresPoll do
  @moduledoc """
  One Postgres table read onto an entry channel by polling (a relay plugin,
  provider id `relay_postgres_poll`): every `interval_ms` its reader reads
  the rows past its cursor, oldest first, `batch_size` at a time, and injects
  each row as one JSON object.

  With only `id_column` the cursor is the id, so new rows are read; with
  `timestamp_column` as well it is the pair, so updated rows come again. The
  plugin builds the query itself from validated, quoted names and runs every
  read in a read-only transaction (`PostgresPoll.Query`). The cursor is the
  binding's internal option `cursor`, committed as the relay's input
  durability says (`PostgresPoll.Reader`), so a restart, a move and a cold start
  continue where it stopped.

  One cursor per binding: not shareable, and refused on a partitioned relay,
  whose members a row's key decides between. Required: the connection's
  `database` and `username`, `table`, `id_column` and an entry channel; the
  rest: `docs/public/plugins/postgres_poll.md`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.Postgres.Connection
  alias Flezha.Plugin.Provider.PostgresPoll.Query
  alias Flezha.Plugin.Provider.PostgresPoll.Reader
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options

  use ProviderBehaviour,
    feeds: true,
    shareable: false,
    telemetry: [caught_up: :event],
    options:
      Connection.rows() ++
        [
          table: [
            type: :string,
            required: true,
            doc: "The table to read: `name` or `schema.name`, case-sensitive"
          ],
          id_column: [
            type: :string,
            required: true,
            doc: "A column that is unique and grows with new rows"
          ],
          timestamp_column: [
            type: :string,
            doc: "A column the writer sets on insert and on update; updated rows come again"
          ],
          columns: [
            type: :names,
            doc: "The columns to read, comma-separated; every column when unset"
          ],
          start_at: [
            type: :enum,
            values: ["beginning", "end"],
            default: "beginning",
            doc:
              "Where a binding with no committed cursor starts: the first row, or after the rows present when it first reads"
          ],
          batch_size: [type: :int, default: 500, min: 1, doc: "Rows in one read"],
          interval_ms: [
            type: :int,
            default: 1_000,
            min: 1,
            doc: "Pause between reads once caught up, and after a failed read"
          ],
          timestamp_delay_ms: [
            type: :int,
            default: 0,
            min: 0,
            doc: "Rows whose timestamp is younger than this are left for a later read"
          ],
          commit_interval_ms: [
            type: :int,
            default: 1_000,
            min: 1,
            doc:
              "Longest a new cursor waits to be committed while full batches are read, unless the relay's input is durable"
          ],
          query_timeout_ms: [
            type: :int,
            default: 30_000,
            min: 1,
            doc: "Statement timeout of one read"
          ],
          # The committed cursor, written back by the reader and read on every
          # restart path, rather than an option a user writes.
          cursor: [type: :any, internal: true]
        ] ++
        Options.channel(:in, "row") ++
        Options.plugin_id("the id it was enabled under") ++
        Options.failure_threshold() ++ Options.idle_after(),
    worker: [module: Reader, restart: :temporary]

  @doc """
  Validate and prepare. Opens no connection: the reader does, at activation, so
  a name refused here never reaches the database. A cursor committed under
  another table or other cursor columns is dropped, and the binding starts from
  `start_at` again.
  """
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         :ok <- ProviderBehaviour.refuse_partitioned(opts, __MODULE__),
         :ok <- Query.validate_names(opts) do
      {:ok,
       opts
       |> Connection.prepare()
       |> Map.merge(%{
         plugin_id: opts.plugin_id || opts[:source_plugin_id],
         cursor: Query.own_cursor(opts.cursor, opts)
       })}
    end
  end

  @doc "One reader per binding, started with `PostgresPoll.Reader.args/1`."
  @impl true
  def worker_specs(opts), do: [{WorkerSup.key(opts), Reader.args(opts)}]

  @doc """
  Teardown is the host's. This callback exists so the provider can be hooked at
  all: every hook requires the matching export.
  """
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  @doc """
  Health verdict from the reader: a missing table or column names itself at
  once, a failure run reaching `failure_threshold` names its error, and a reader
  whose last settled read is older than the idle window says how old.
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

  @doc "What `flz relay info` shows beneath the plugin: the table and its verdict."
  @impl true
  def inspect_state(opts), do: %{table: opts.table, health: health(opts)}

  defp reader_health(pid, opts) do
    case Reader.probe(pid) do
      {:ok, facts} -> verdict(facts, opts)
      :timeout -> {:fail_open, :probe_timeout}
    end
  end

  defp verdict(%{missing: missing}, _opts) when missing != nil, do: {:unhealthy, missing}

  defp verdict(%{consecutive_failures: failures, last_error: error}, opts)
       when failures >= opts.failure_threshold,
       do: {:unhealthy, {:read_failed, error}}

  defp verdict(%{last_settled_at: settled_at}, opts),
    do: ProviderBehaviour.idle_verdict(settled_at, opts.idle_after_ms, opts.interval_ms)
end
