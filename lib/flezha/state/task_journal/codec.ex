defmodule Flezha.State.TaskJournal.Codec do
  @moduledoc """
  The `task.journal` entry format: the type numbering, and the bytes each type
  carries.

  One entry point each way — `encode_entry/4` and `decode_entry/3` — over the
  change-bytes of one WAL record. The layouts are in
  `docs/internals/state_persistence.md`; this module is their only
  implementation, and `Flezha.State.TaskJournal` reads its numbering rather than
  keeping a second copy.

  A decode never raises: an unknown type, a truncated payload or a malformed
  count answers `{:error, reason}`, because the caller is folding a file whose
  tail may have been torn by a crash.
  """

  @type_task_committed 1
  @type_outbox_flushed 2
  @type_iter_committed 3
  @type_compensated 5
  @type_rewound 6
  @entry_types %{
    @type_task_committed => :task_committed,
    @type_outbox_flushed => :outbox_flushed,
    @type_iter_committed => :iter_committed,
    @type_compensated => :compensated,
    @type_rewound => :rewound
  }

  @doc "The on-disk type byte for an entry type name."
  @spec type_code(atom()) :: pos_integer()
  def type_code(name) do
    {code, ^name} = Enum.find(@entry_types, fn {_code, n} -> n == name end)
    code
  end

  alias Flezha.State.TaskJournal

  @doc false
  @spec encode_entry(pos_integer(), non_neg_integer(), non_neg_integer(), iodata()) :: binary()
  def encode_entry(type, task_idx, iter, payload)
      when type in 1..255 and task_idx in 0..0xFFFF and iter in 0..0xFFFF_FFFF do
    IO.iodata_to_binary([<<type::8, task_idx::16-big, iter::32-big>>, payload])
  end

  @doc false
  @spec decode_entry(non_neg_integer(), binary(), non_neg_integer()) ::
          {:ok, TaskJournal.entry()} | {:error, term()}
  def decode_entry(seq, body, epoch \\ 0)

  def decode_entry(seq, <<type::8, task_idx::16-big, iter::32-big, payload::binary>>, epoch) do
    entry = %{seq: seq, epoch: epoch, task_idx: task_idx, iter: iter}

    with {:ok, name} <- entry_type(type),
         {:ok, decoded} <- decode_payload(name, payload) do
      {:ok, Map.merge(entry, %{type: name, payload: decoded})}
    end
  end

  def decode_entry(_seq, _short, _epoch), do: {:error, :short_entry}

  def entry_type(type) do
    case Map.fetch(@entry_types, type) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, {:unknown_entry_type, type}}
    end
  end

  def entry_type_code(:task_committed), do: @type_task_committed
  def entry_type_code(:compensated), do: @type_compensated

  defp decode_payload(:task_committed, <<n_cells::16-big, rest::binary>>) do
    with {:ok, cells, rest} <- decode_cells(rest, n_cells, []),
         {:ok, handoff, rest} <- decode_handoff(rest),
         {:ok, outbox, rest} <- decode_outbox(rest),
         {:ok, completion} <- decode_completion(rest) do
      {:ok, %{cells_ran: cells, handoff: handoff, outbox: outbox, completion: completion}}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(
         :iter_committed,
         <<offset::binary-size(9), chan_len::16-big, entry_channel::binary-size(chan_len),
           rest::binary>>
       ) do
    with {:ok, consumed_offset} <- decode_offset(offset),
         {:ok, handoff, rest} <- decode_handoff(rest),
         {:ok, completion} <- decode_completion(rest) do
      {:ok,
       %{
         consumed_offset: consumed_offset,
         entry_channel: entry_channel,
         handoff: handoff,
         completion: completion
       }}
    else
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:compensated, payload) do
    case decode_outbox(payload) do
      {:ok, outbox, <<>>} -> {:ok, %{outbox: outbox}}
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:outbox_flushed, <<for_type::8>>) do
    with {:ok, name} <- entry_type(for_type), do: {:ok, %{for_type: name}}
  end

  # The rewind target is the entry's own `iter` field — no payload to carry.
  defp decode_payload(:rewound, <<>>), do: {:ok, %{}}

  defp decode_payload(_name, _payload), do: {:error, :bad_payload}

  def encode_committed_payload(cells_ran, handoff) do
    [
      <<length(cells_ran)::16-big>>,
      Enum.map(cells_ran, fn id -> [<<byte_size(id)::16-big>>, id] end),
      encode_handoff(handoff)
    ]
  end

  @doc """
  A consumed input-log offset or its absence, as an `iter_committed` payload
  carries it: one presence byte, then the offset. `0` is the
  log's first offset, so it cannot stand for "none".
  """
  @spec encode_offset(non_neg_integer() | nil) :: binary()
  def encode_offset(nil), do: <<0, 0::64-big>>
  def encode_offset(offset) when is_integer(offset) and offset >= 0, do: <<1, offset::64-big>>

  defp decode_offset(<<0, 0::64-big>>), do: {:ok, nil}
  defp decode_offset(<<1, offset::64-big>>), do: {:ok, offset}
  defp decode_offset(_), do: {:error, :bad_payload}

  @doc """
  The trailing completion segment of a `task_committed` or `iter_committed`
  payload: empty for none, so an entry written before completions existed
  reads as not completing.
  """
  def encode_completion(nil), do: <<>>
  def encode_completion(result), do: <<1, byte_size(result)::32-big, result::binary>>

  defp decode_completion(<<>>), do: {:ok, nil}
  defp decode_completion(<<1, len::32-big, result::binary-size(len)>>), do: {:ok, result}
  defp decode_completion(_), do: {:error, :bad_payload}

  def encode_handoff(handoff) do
    [
      <<length(handoff)::32-big>>,
      Enum.map(handoff, fn {channel_id, bytes} ->
        [<<channel_id::32-big, byte_size(bytes)::32-big>>, bytes]
      end)
    ]
  end

  def encode_outbox(outbox) do
    [
      <<length(outbox)::32-big>>,
      Enum.map(outbox, fn item ->
        [
          <<byte_size(item.cell_id)::16-big>>,
          item.cell_id,
          <<item.seq_in_task::32-big, byte_size(item.channel)::16-big>>,
          item.channel,
          <<byte_size(item.payload)::32-big>>,
          item.payload
        ]
      end)
    ]
  end

  defp decode_cells(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_cells(<<len::16-big, id::binary-size(len), rest::binary>>, n, acc),
    do: decode_cells(rest, n - 1, [id | acc])

  defp decode_cells(_, _, _), do: {:error, :bad_payload}

  defp decode_handoff(<<n::32-big, rest::binary>>), do: decode_handoff_items(rest, n, [])
  defp decode_handoff(_), do: {:error, :bad_payload}

  defp decode_handoff_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_handoff_items(
         <<channel_id::32-big, len::32-big, bytes::binary-size(len), rest::binary>>,
         n,
         acc
       ),
       do: decode_handoff_items(rest, n - 1, [{channel_id, bytes} | acc])

  defp decode_handoff_items(_, _, _), do: {:error, :bad_payload}

  defp decode_outbox(<<n::32-big, rest::binary>>), do: decode_outbox_items(rest, n, [])
  defp decode_outbox(_), do: {:error, :bad_payload}

  defp decode_outbox_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_outbox_items(
         <<seg_len::16-big, cell_id::binary-size(seg_len), seq_in_task::32-big, chan_len::16-big,
           channel::binary-size(chan_len), payload_len::32-big, payload::binary-size(payload_len),
           rest::binary>>,
         n,
         acc
       ) do
    item = %{
      cell_id: cell_id,
      seq_in_task: seq_in_task,
      channel: channel,
      payload: payload
    }

    decode_outbox_items(rest, n - 1, [item | acc])
  end

  defp decode_outbox_items(_, _, _), do: {:error, :bad_payload}
end
