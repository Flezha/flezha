defmodule Flezha.State.Wire do
  @moduledoc """
  Wire-protocol codec for the per-Cell persistence path.

  Two payload-id strings are reserved on the data stream, both SDK ->
  controller: `"cell_state_change"`, carrying one Change record destined for
  the per-Cell WAL, and `"cell_state_snapshot"`, carrying a serialized
  `Self::State` produced by a controller-initiated `CompactCell`. Both share
  one frame structure — the difference is only what the body means — and that
  structure is the protocol's.

  The other payload ids a runner writes in the same frame layout are
  `cell_effect_record` (one invocation's effect record under the
  effectively-once tier, `decode_effect_body/1`), `compensation_intent` and
  `compensation_done` (a steps relay's saga change-log) and `park_payload` (a
  dead-letter park, `decode_park_body/1`); `delete_cell_state` carries the bare
  `cell_id` alone.

  `cell_id_bytes` is structured rather than opaque so the controller can route
  a frame to the right per-Cell `WalWriter` without a side table. The
  controller's `runner_id` is implicit: the controller knows which runner sent
  the frame from the connection it arrived on.
  """

  alias Flezha.State.Frame

  @header_size Frame.header_size()
  @format_id_effect_record Frame.format_id(:effect_record)

  @doc """
  Encode a `cell_id` triple into its on-wire byte form.
  """
  @spec encode_cell_id(binary(), binary(), binary()) :: binary()
  def encode_cell_id(relay_class, tasklet_id, cell_key)
      when is_binary(relay_class) and is_binary(tasklet_id) and is_binary(cell_key) do
    rc_len = byte_size(relay_class)
    sh_len = byte_size(tasklet_id)
    pk_len = byte_size(cell_key)

    if rc_len > 255 or sh_len > 255 do
      raise ArgumentError, "relay_class / tasklet_id must be ≤ 255 bytes"
    end

    if pk_len > 0xFFFF do
      raise ArgumentError, "cell_key must be ≤ 65535 bytes"
    end

    <<rc_len::8, relay_class::binary, sh_len::8, tasklet_id::binary, pk_len::16-big,
      cell_key::binary>>
  end

  @doc """
  Decode the on-wire `cell_id_bytes` into `{relay_class, tasklet_id, cell_key}`.

  The relay class and tasklet id each become one directory under the state
  root, so a value that would name another directory (`path_segment?/1`) is
  refused here, before any path is built from it. The tasklet id alone may be
  empty: a relay-level frame, such as a processed prefix, names no tasklet.
  """
  @spec decode_cell_id(binary()) :: {:ok, {binary(), binary(), binary()}} | {:error, term()}
  def decode_cell_id(
        <<rc_len::8, relay_class::binary-size(rc_len), sh_len::8, tasklet_id::binary-size(sh_len),
          pk_len::16-big, cell_key::binary-size(pk_len)>>
      ) do
    if path_segment?(relay_class) and (tasklet_id == "" or path_segment?(tasklet_id)),
      do: {:ok, {relay_class, tasklet_id, cell_key}},
      else: {:error, :bad_cell_id}
  end

  def decode_cell_id(_), do: {:error, :bad_cell_id}

  @doc """
  Whether `segment` names exactly one directory level: not empty, `.` or `..`,
  and holding no `/` or NUL. Binary matching only, as it runs per state frame.
  """
  @spec path_segment?(binary()) :: boolean()
  def path_segment?(segment) when segment in ["", ".", ".."], do: false
  def path_segment?(segment), do: :binary.match(segment, ["/", <<0>>]) == :nomatch

  @doc """
  Decode an inbound `cell_state_change` / `cell_state_snapshot`
  frame.

  Returns `{:ok, %{cell_id: {relay_class, tasklet_id, cell_key},
                   seq: u64, header: <<_::128>>, body: bytes}}` or an error.
  Verifies the trailing crc32 over the body.
  """
  @spec decode_frame(binary()) ::
          {:ok,
           %{
             cell_id: {binary(), binary(), binary()},
             seq: non_neg_integer(),
             header: <<_::128>>,
             body: binary()
           }}
          | {:error, term()}
  def decode_frame(<<cell_id_len::16-big, rest::binary>>) do
    case rest do
      <<cell_id_bytes::binary-size(^cell_id_len), seq::64-big, header::binary-size(@header_size),
        body_len::32-big, body::binary-size(body_len), crc::32-big>> ->
        if :erlang.crc32(body) == crc do
          case decode_cell_id(cell_id_bytes) do
            {:ok, identity} ->
              {:ok, %{cell_id: identity, seq: seq, header: header, body: body}}

            err ->
              err
          end
        else
          {:error, :crc_mismatch}
        end

      _ ->
        {:error, :short_read}
    end
  end

  def decode_frame(_), do: {:error, :short_read}

  @doc """
  Whether a WAL record header marks an effect record (`format_id = 2` at
  header byte 9) — the durable marker that keeps mixed WALs readable.
  """
  @spec effect_record_header?(binary()) :: boolean()
  def effect_record_header?(<<_::binary-size(9), format_id::8, _::binary>>),
    do: format_id == @format_id_effect_record

  def effect_record_header?(_), do: false

  @doc """
  Decode a postcard `EffectRecordBody` (the body of a
  `cell_effect_record` frame / format-2 WAL record):

  ```
  changes:         Vec<Vec<u8>>            — postcard S::Change bytes each
  outputs:         Vec<OutputRecord>       — {channel, key, metadata, payload}
  consumed_offset: Option<u64>
  dlq:             Option<DlqEntryBody>    — {channel, error, attempts, payload}
  event_time:      u64                     — the invocation's declared clock value
  timers:          Vec<TimerOp>            — {kind, scheduled_at, register}
  ```

  `event_time` is the extracted event time under an `EventTimeFrom` clock —
  the column a late-input correction resolves its baseline seq against.

  `timers` carries the SDK's mutations of the Cell's pending-timer set. The
  controller stores them and hands them back on restore without interpreting
  them — the same property that makes `changes` SDK-owned bytes — but decoding
  the field is what keeps the body's trailing-byte check honest.

  Postcard encodes `Vec`/`String` as a LEB128 varint length + contents,
  tuples/structs as concatenated fields, and `Option` as `0x00 | 0x01 ++ T`.
  """
  @spec decode_effect_body(binary()) ::
          {:ok,
           %{
             changes: [binary()],
             outputs: [
               %{
                 channel: binary(),
                 key: binary(),
                 metadata: [{binary(), binary()}],
                 payload: binary()
               }
             ],
             consumed_offset: non_neg_integer() | nil,
             dlq: map() | nil,
             event_time: non_neg_integer(),
             timers: [
               %{
                 kind: :event_time | :processing_time,
                 scheduled_at: non_neg_integer(),
                 register: boolean()
               }
             ],
             edges: [edge()]
           }}
          | {:error, term()}
  def decode_effect_body(body) when is_binary(body) do
    with {:ok, changes, rest} <- decode_vec(body, &decode_bytes/1),
         {:ok, outputs, rest} <- decode_vec(rest, &decode_output/1),
         {:ok, consumed_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, dlq, rest} <- decode_option(rest, &decode_dlq_entry/1),
         {:ok, event_time, rest} <- decode_varint(rest),
         {:ok, timers, rest} <- decode_vec(rest, &decode_timer_op/1),
         {:ok, edges, <<>>} <- decode_vec(rest, &decode_edge/1) do
      {:ok,
       %{
         changes: changes,
         outputs: outputs,
         consumed_offset: consumed_offset,
         dlq: dlq,
         event_time: event_time,
         timers: timers,
         edges: edges
       }}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @typedoc """
  One inter-tasklet value an effect record keeps: the channel it was emitted
  on and the consuming tasklet, the input-log offset it derives from, and the
  value through the channel's codec.
  """
  @type edge :: %{
          channel: non_neg_integer(),
          consumer: non_neg_integer(),
          origin: non_neg_integer() | nil,
          payload: binary()
        }

  defp decode_edge(bin) do
    with {:ok, channel, rest} <- decode_varint(bin),
         {:ok, consumer, rest} <- decode_varint(rest),
         {:ok, origin, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, payload, rest} <- decode_bytes(rest) do
      {:ok, %{channel: channel, consumer: consumer, origin: origin, payload: payload}, rest}
    end
  end

  @typedoc "One mutation of a Cell's pending-timer set, as the postcard `TimerOp` carries it."
  @type timer_op :: %{
          kind: :event_time | :processing_time,
          scheduled_at: non_neg_integer(),
          register: boolean()
        }

  @doc """
  Decode a postcard `ParkBody` (the body of a `park_payload` frame,
  tier-off dead-letter park):

  ```
  park_id:       String              — SDK-minted ack-correlation id
  origin_offset: Option<u64>
  entry:         DlqEntryBody        — {channel, error, attempts, payload},
                                       the same record a tier-on park embeds
                                       in its effect record
  reply_to:      Option<String>      — the token of the outside call waiting
                                       on the parked input
  ```

  Returned flattened (`park_id`, `origin_offset` and `reply_to` merged into
  the entry map) — the store consumes one shape for both park paths.
  """
  @spec decode_park_body(binary()) ::
          {:ok,
           %{
             park_id: binary(),
             channel: binary(),
             origin_offset: non_neg_integer() | nil,
             error: binary(),
             attempts: non_neg_integer(),
             payload: binary() | nil,
             reply_to: binary() | nil
           }}
          | {:error, term()}
  def decode_park_body(body) when is_binary(body) do
    with {:ok, park_id, rest} <- decode_bytes(body),
         {:ok, origin_offset, rest} <- decode_option(rest, &decode_varint/1),
         {:ok, entry, rest} <- decode_dlq_entry(rest),
         {:ok, reply_to, <<>>} <- decode_option(rest, &decode_bytes/1) do
      {:ok,
       Map.merge(entry, %{park_id: park_id, origin_offset: origin_offset, reply_to: reply_to})}
    else
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  @doc """
  Decode a WAL record as an effect record, or `nil` when it is not one (a plain
  change record, or a body this build cannot read).

  The header check and the body decode always travel together — every reader
  that wants an effect record's fields wants exactly this — so they live here,
  beside the codec they belong to, rather than being re-paired at each call site.
  """
  @spec decode_effect_record(binary(), binary()) :: map() | nil
  def decode_effect_record(header, body) do
    with true <- effect_record_header?(header),
         {:ok, decoded} <- decode_effect_body(body) do
      decoded
    else
      _ -> nil
    end
  end

  @doc """
  Decode a postcard `CorrectionRequestBody` (the body of a `correction_request`
  frame): a single `target_event_time: u64`. The Cell identity rides the
  frame envelope, so the body carries nothing else — the controller re-reads the
  input-log range itself, because the late input is already in it.
  """
  @spec decode_correction_request(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def decode_correction_request(body) when is_binary(body) do
    case decode_varint(body) do
      {:ok, target, <<>>} -> {:ok, target}
      {:ok, _, _trailing} -> {:error, :trailing_bytes}
      {:error, _} = err -> err
    end
  end

  # ---------- postcard primitives ----------

  defp decode_vec(bin, item_fun) do
    case decode_varint(bin) do
      {:ok, count, rest} -> decode_items(rest, count, item_fun, [])
      err -> err
    end
  end

  defp decode_items(rest, 0, _fun, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_items(rest, n, fun, acc) do
    case fun.(rest) do
      {:ok, item, rest} -> decode_items(rest, n - 1, fun, [item | acc])
      err -> err
    end
  end

  defp decode_bytes(bin) do
    with {:ok, len, rest} <- decode_varint(bin),
         <<bytes::binary-size(^len), rest::binary>> <- rest do
      {:ok, bytes, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :short_read}
    end
  end

  defp decode_output(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, key, rest} <- decode_bytes(rest),
         {:ok, metadata, rest} <- decode_vec(rest, &decode_pair/1),
         {:ok, payload, rest} <- decode_bytes(rest) do
      {:ok, %{channel: channel, key: key, metadata: metadata, payload: payload}, rest}
    end
  end

  defp decode_pair(bin) do
    with {:ok, k, rest} <- decode_bytes(bin),
         {:ok, v, rest} <- decode_bytes(rest) do
      {:ok, {k, v}, rest}
    end
  end

  defp decode_dlq_entry(bin) do
    with {:ok, channel, rest} <- decode_bytes(bin),
         {:ok, error, rest} <- decode_bytes(rest),
         {:ok, attempts, rest} <- decode_varint(rest),
         {:ok, payload, rest} <- decode_option(rest, &decode_bytes/1) do
      {:ok, %{channel: channel, error: error, attempts: attempts, payload: payload}, rest}
    end
  end

  # One pending-timer mutation: `[kind varint][scheduled_at varint][register u8]`.
  # The kind's ordinal order is the SDKs' `TimerKind`: 0 event time, 1 processing
  # time.
  defp decode_timer_op(bin) do
    with {:ok, kind, rest} <- decode_varint(bin),
         {:ok, scheduled_at, rest} <- decode_varint(rest),
         <<register::8, rest::binary>> <- rest,
         true <- register <= 1 do
      {:ok, %{kind: timer_kind(kind), scheduled_at: scheduled_at, register: register == 1}, rest}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_timer_op}
    end
  end

  defp timer_kind(0), do: :event_time
  defp timer_kind(1), do: :processing_time
  defp timer_kind(other), do: {:unknown, other}

  defp decode_option(<<0, rest::binary>>, _item_fun), do: {:ok, nil, rest}
  defp decode_option(<<1, rest::binary>>, item_fun), do: item_fun.(rest)
  defp decode_option(_, _), do: {:error, :bad_option}

  defp decode_varint(bin, shift \\ 0, acc \\ 0)

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when byte < 128,
    do: {:ok, acc + Bitwise.bsl(byte, shift), rest}

  defp decode_varint(<<byte::8, rest::binary>>, shift, acc) when shift < 64,
    do: decode_varint(rest, shift + 7, acc + Bitwise.bsl(byte - 128, shift))

  defp decode_varint(_, _, _), do: {:error, :bad_varint}
end
