defmodule Flezha.State.WalWriter.State do
  @moduledoc """
  The per-Cell writer's struct, and the clock every part of it stamps with.

  Split out of `Flezha.State.WalWriter` so its `Boot`, `Settlement`, `Epoch` and
  `Admission` siblings can thread the same struct without naming the GenServer
  that owns it — a sibling naming its parent is a dependency cycle, and the
  struct is the one thing all five genuinely share.

  Field groups, in the order they appear: identity and the open file; the
  liveness cursors the `Watchdog` reads; the resolved `limits`; back-pressure
  and credit; the effectively-once outbox; and the two per-file indexes.
  """

  alias Flezha.SDK.RecordFrame
  alias Flezha.State.{Paths, SeqIndex}

  defstruct [
    :cell_id,
    :file_handle,
    :wal_path,
    :last_persisted_seq,
    :last_advance_at,
    :last_change_seen_at,
    :owning_runner,
    :pending_bytes,
    :pending_since_us,
    # The bounds resolved once at `init/1` and never re-read: the soft/low/hard
    # un-fsynced byte watermarks, the un-credited mailbox-depth cap, and the
    # outbox's confirmation window and resettle interval.
    :limits,
    :metrics,
    :latency_reservoir,
    :backpressured?,
    :shedding?,
    :gap_open?,
    :awaiting_baseline?,
    :oldest_unsnapshotted_at,
    :credit_pending,
    :credit_active?,
    :refused_sessions,
    # Effectively-once outbox: FIFO of effect-record entries awaiting the
    # fsync-gated `:on_output` dispatch (`%{seq, outputs, consumed_offset}`,
    # dispatchable once `seq <= outbox_durable_seq`), the flush cursor, the
    # realign epoch, and the retry timer armed while the operative's
    # Overwatch is not yet resolvable.
    :pending_outbox,
    :outbox_durable_seq,
    :outbox_flushed_seq,
    :outbox_epoch,
    :timers,
    # Dispatched but not yet confirmed by the sinks: the per-entry tallies
    # (`Flezha.State.Outbox` keys) and, for a stream writer, the dispatch-order
    # queue of `{seq, at}` the cursor drains in. The cursor may only reach a seq
    # once every earlier one has left this queue, which is what stops an
    # out-of-order confirmation from advancing it past unconfirmed output.
    :outbox_awaiting,
    :outbox_inflight,
    # Highest seq of an OUTPUT-BEARING effect record queued. Producing-liveness
    # compares it against `outbox_flushed_seq` to see an outbox that accumulates
    # without draining. It must count only output-bearing records:
    # `outbox_durable_seq` advances on every fsynced record, so a Cell that
    # persists state but emits nothing would read as a frozen outbox forever.
    :outbox_last_seq,
    # A `Flezha.State.SeqIndex` over the file: a cut at either end finds its
    # boundary within one sample span and copies only the kept side.
    :seq_index,
    # Batched appends owed their reply: `{from, reply}` newest first, answered
    # in append order once the records are synced.
    :sync_waiters,
    # `header => seq` of every record still in the file, for a log whose
    # appends dedup on their header (a record log's barrier or inbox entry).
    # Built from the file on the first keyed append, never for a stream WAL.
    :header_index,
    # The record this writer's file refused (`Flezha.State.RecordFrame`), or
    # `nil`. Set, the writer stays alive and answers every request that would
    # write or cut with the refusal: the file holds records the controller
    # acknowledged and cannot parse past, and a `:transient` child that raised
    # here would be restarted straight back into it.
    :refusal
  ]

  @type t :: %__MODULE__{
          cell_id: Paths.cell_id(),
          file_handle: :file.io_device(),
          wal_path: binary(),
          last_persisted_seq: integer(),
          last_advance_at: integer(),
          last_change_seen_at: integer(),
          owning_runner: binary() | nil,
          pending_bytes: non_neg_integer(),
          pending_since_us: non_neg_integer() | nil,
          limits: Admission.limits(),
          metrics: :counters.counters_ref(),
          latency_reservoir: :queue.queue(),
          backpressured?: boolean(),
          shedding?: boolean(),
          gap_open?: boolean(),
          awaiting_baseline?: boolean(),
          oldest_unsnapshotted_at: integer() | nil,
          credit_pending: %{optional({pid(), binary()}) => pos_integer()},
          credit_active?: boolean(),
          refused_sessions: MapSet.t(binary()),
          pending_outbox: :queue.queue(map()),
          outbox_durable_seq: integer(),
          outbox_flushed_seq: integer(),
          outbox_epoch: non_neg_integer(),
          timers: Flezha.Process.Timer.timers(),
          outbox_awaiting: %{optional(Outbox.key()) => map()},
          outbox_inflight: :queue.queue({non_neg_integer(), integer()}),
          outbox_last_seq: integer(),
          seq_index: SeqIndex.t(),
          sync_waiters: [{GenServer.from(), term()}],
          header_index: %{optional(binary()) => non_neg_integer()} | nil,
          refusal: RecordFrame.refusal() | nil
        }

  @type limits :: %{
          high_watermark: pos_integer(),
          low_watermark: pos_integer(),
          hard_watermark: pos_integer(),
          pending_msg_cap: pos_integer(),
          outbox_inflight_max: pos_integer(),
          outbox_resettle_ms: pos_integer()
        }

  @typedoc """
  A Cell's durable progress, read as one value: how far its records are
  persisted, how far they are fsynced, how far its outbox has been confirmed,
  and the realign epoch all three are counted under.
  """
  @type progress :: %{
          last_persisted_seq: integer(),
          outbox_durable_seq: integer(),
          outbox_flushed_seq: integer(),
          epoch: non_neg_integer()
        }

  @doc """
  The Cell's durable cursors as one record.

  The one accessor: a reader that needs a Cell's progress takes this rather
  than copying the writer's fields, so the four cursors cannot be read at four
  different moments and reported as one picture.
  """
  @spec progress(t()) :: progress()
  def progress(%__MODULE__{} = state) do
    %{
      last_persisted_seq: state.last_persisted_seq,
      outbox_durable_seq: state.outbox_durable_seq,
      outbox_flushed_seq: state.outbox_flushed_seq,
      epoch: state.outbox_epoch
    }
  end

  @doc """
  Whether `progress` holds the ordering the cursors assume: an output is
  confirmed only after its record is fsynced, and a record is fsynced only
  after it is appended, so `outbox_flushed_seq <= outbox_durable_seq <=
  last_persisted_seq`.

  The snapshot cursor is deliberately not in that chain. It counts how much
  state a compaction folded away, the flush cursor how much output was
  confirmed; a Cell that emits nothing snapshots past a flush cursor that
  never leaves `-1`, and ordering the two against each other would be false of
  every Cell outside the effectively-once tier.
  """
  @spec ordered?(progress()) :: boolean()
  def ordered?(%{
        last_persisted_seq: persisted,
        outbox_durable_seq: durable,
        outbox_flushed_seq: flushed
      }),
      do: flushed <= durable and durable <= persisted

  @doc "The writer family's monotonic clock, in milliseconds."
  def now_ms(), do: :erlang.monotonic_time(:millisecond)
end
