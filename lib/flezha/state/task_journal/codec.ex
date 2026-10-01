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
  @type_iter_committed 3
  @type_compensated 5
  @type_rewound 6
  @type_input_released 7
  @type_dead_lettered 8
  @entry_types %{
    @type_task_committed => :task_committed,
    @type_iter_committed => :iter_committed,
    @type_compensated => :compensated,
    @type_rewound => :rewound,
    @type_input_released => :input_released,
    @type_dead_lettered => :dead_lettered
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

  defp decode_payload(:task_committed, payload) do
    with {:ok, cells, rest} <- decode_cells(payload),
         {:ok, handoff, rest} <- decode_handoff(rest),
         {:ok, completion, rest} <- take_completion(rest),
         {:ok, trigger, rest} <- take_trigger(rest),
         {:ok, exit} <- decode_exit(rest) do
      {:ok,
       %{
         cells_ran: cells,
         handoff: handoff,
         completion: completion,
         trigger: trigger,
         exit: exit
       }}
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
         {:ok, completion, rest} <- take_completion(rest),
         {:ok, failed_turns, rest} <- take_failed_turns(rest),
         {:ok, exit} <- decode_exit(rest) do
      {:ok,
       %{
         consumed_offset: consumed_offset,
         entry_channel: entry_channel,
         handoff: handoff,
         completion: completion,
         failed_turns: failed_turns,
         exit: exit
       }}
    else
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(
         :dead_lettered,
         <<offset::binary-size(9), rest::binary>>
       ) do
    with {:ok, consumed_offset} <- decode_offset(offset),
         {:ok, [entry_channel, tasklet], rest} <- decode_strings(rest, 2, []),
         <<attempts::32-big, rest::binary>> <- rest,
         {:ok, [dlq_entry], <<count::32-big>>} <- decode_strings(rest, 1, []) do
      {:ok,
       %{
         consumed_offset: consumed_offset,
         entry_channel: entry_channel,
         tasklet: tasklet,
         attempts: attempts,
         dlq_entry: dlq_entry,
         count: count
       }}
    else
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:compensated, payload) do
    case decode_cells(payload) do
      {:ok, cells, <<>>} -> {:ok, %{cells_ran: cells}}
      _ -> {:error, :bad_payload}
    end
  end

  # The rewind target is the entry's own `iter` field — no payload to carry.
  defp decode_payload(:rewound, <<>>), do: {:ok, %{}}

  defp decode_payload(:input_released, <<offset::binary-size(9)>>) do
    case decode_offset(offset) do
      {:ok, consumed_offset} when is_integer(consumed_offset) ->
        {:ok, %{consumed_offset: consumed_offset}}

      _ ->
        {:error, :bad_payload}
    end
  end

  defp decode_payload(_name, _payload), do: {:error, :bad_payload}

  @doc """
  The Cells a stage entry names: each identity, the effect records it wrote in
  the stage (`first_seq..seq`, both `nil` when it wrote none) and the plugin
  channels they wrote on.
  """
  @spec encode_cells([TaskJournal.committed_cell()]) :: iodata()
  def encode_cells(cells) do
    [<<length(cells)::16-big>>, Enum.map(cells, &encode_cell/1)]
  end

  defp encode_cell(%{cell_id: id, first_seq: first, seq: last, channels: channels}) do
    records = if is_integer(last), do: <<1, first::64-big, last::64-big>>, else: <<0>>

    [
      <<byte_size(id)::16-big>>,
      id,
      records,
      <<length(channels)::16-big>>,
      Enum.map(channels, &[<<byte_size(&1)::16-big>>, &1])
    ]
  end

  defp decode_cells(<<n::16-big, rest::binary>>), do: decode_cells(rest, n, [])
  defp decode_cells(_), do: {:error, :bad_payload}

  defp decode_cells(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_cells(<<len::16-big, id::binary-size(len), rest::binary>>, n, acc) do
    with {:ok, first, last, rest} <- decode_records(rest),
         <<n_chan::16-big, rest::binary>> <- rest,
         {:ok, channels, rest} <- decode_strings(rest, n_chan, []) do
      cell = %{cell_id: id, first_seq: first, seq: last, channels: channels}
      decode_cells(rest, n - 1, [cell | acc])
    else
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_cells(_, _, _), do: {:error, :bad_payload}

  defp decode_records(<<0, rest::binary>>), do: {:ok, nil, nil, rest}

  defp decode_records(<<1, first::64-big, last::64-big, rest::binary>>),
    do: {:ok, first, last, rest}

  defp decode_records(_), do: {:error, :bad_payload}

  defp decode_strings(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_strings(<<len::16-big, value::binary-size(len), rest::binary>>, n, acc),
    do: decode_strings(rest, n - 1, [value | acc])

  defp decode_strings(_, _, _), do: {:error, :bad_payload}

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

  defp take_completion(<<1, len::32-big, result::binary-size(len), rest::binary>>),
    do: {:ok, result, rest}

  defp take_completion(rest), do: {:ok, nil, rest}

  @doc """
  A `dead_lettered` payload: the consumed offset, the entry channel, the
  failing tasklet, its attempts, the DLQ entry the turn was parked as and the
  instance's running count of dead-lettered turns, this one included.
  """
  @spec encode_dead_letter(
          non_neg_integer() | nil,
          binary(),
          binary(),
          non_neg_integer(),
          binary(),
          pos_integer()
        ) :: iodata()
  def encode_dead_letter(consumed_offset, entry_channel, tasklet, attempts, dlq_entry, count),
    do: [
      encode_offset(consumed_offset),
      Enum.map([entry_channel, tasklet], &[<<byte_size(&1)::16-big>>, &1]),
      <<attempts::32-big, byte_size(dlq_entry)::16-big>>,
      dlq_entry,
      <<count::32-big>>
    ]

  @doc """
  The trailing segment of an `iter_committed` payload, after the completion:
  the instance's running count of dead-lettered turns and the iteration of the
  latest, so the count outlives a journal cut. Empty while none was, which
  keeps every other entry's bytes as they were.
  """
  @spec encode_failed_turns({non_neg_integer(), non_neg_integer()} | nil) :: binary()
  def encode_failed_turns(nil), do: <<>>
  def encode_failed_turns({count, last}), do: <<3, count::32-big, last::32-big>>

  defp take_failed_turns(<<3, count::32-big, last::32-big, rest::binary>>),
    do: {:ok, {count, last}, rest}

  defp take_failed_turns(rest), do: {:ok, nil, rest}

  @doc """
  The trailing exit segment of a `task_committed` or `iter_committed` payload:
  a stepped scope's exit values the commit made final, `{channel, values}`,
  which the controller logs into its outer instance's input log; empty on
  every other entry.
  """
  @spec encode_exit({binary(), [binary()]} | nil) :: binary()
  def encode_exit(nil), do: <<>>

  def encode_exit({channel, values}) do
    term = :erlang.term_to_binary({channel, values})
    <<4, byte_size(term)::32-big, term::binary>>
  end

  defp decode_exit(<<>>), do: {:ok, nil}

  defp decode_exit(<<4, size::32-big, term::binary-size(size)>>) do
    case :erlang.binary_to_term(term, [:safe]) do
      {channel, values} when is_binary(channel) and is_list(values) -> {:ok, {channel, values}}
      _other -> {:error, :bad_payload}
    end
  rescue
    ArgumentError -> {:error, :bad_payload}
  end

  defp decode_exit(_), do: {:error, :bad_payload}

  @doc """
  The trailing trigger segment of a `task_committed` payload, after the
  completion: the timer firing that opened the pass this tasklet was the root
  of, empty on every other entry.
  """
  def encode_trigger(nil), do: <<>>

  def encode_trigger(%{cell_key: cell_key, kind: kind, scheduled_at: at}),
    do: <<2, byte_size(cell_key)::16-big, cell_key::binary, kind_code(kind), at::64-big>>

  defp take_trigger(
         <<2, key_len::16-big, cell_key::binary-size(key_len), kind, at::64-big, rest::binary>>
       )
       when kind in [0, 1],
       do: {:ok, %{cell_key: cell_key, kind: kind_name(kind), scheduled_at: at}, rest}

  defp take_trigger(rest), do: {:ok, nil, rest}

  defp kind_code(:processing_time), do: 0
  defp kind_code(:event_time), do: 1
  defp kind_name(0), do: :processing_time
  defp kind_name(1), do: :event_time

  def encode_handoff(handoff) do
    [
      <<length(handoff)::32-big>>,
      Enum.map(handoff, fn {channel_id, bytes} ->
        [<<channel_id::32-big, byte_size(bytes)::32-big>>, bytes]
      end)
    ]
  end

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
end
