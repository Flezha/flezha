defmodule Flezha.Observability.AuditLog.Reader do
  @moduledoc """
  Streaming reader for audit JSONL files.

  Runs in the caller process — never the Writer. Opens the file with
  `[:read, :raw, :binary]`; transparently handles `.jsonl.gz` files by
  wrapping the byte stream in `:zlib` streaming decompression.

  Filters are applied during the stream so memory is bounded:

    * `:source` — string source (e.g. `"observability_service"`)
    * `:name` — string name
    * `:since_us` — `ts_us >= since_us`
    * `:until_us` — `ts_us <= until_us`
    * `:limit` — stop after N matching records

  A truncated last line (no trailing `\\n`) is skipped silently.
  """

  alias Flezha.Observability.AuditLog.Codec

  @type opts :: [
          source: String.t() | nil,
          name: String.t() | nil,
          since_us: integer() | nil,
          until_us: integer() | nil,
          limit: pos_integer() | nil
        ]

  @doc """
  Return an `Enumerable` that yields one decoded record per element.
  """
  @spec stream(String.t(), opts()) :: Enumerable.t()
  def stream(path, opts \\ []) when is_binary(path) do
    Stream.resource(
      fn -> init_state(path, opts) end,
      &next/1,
      &cleanup/1
    )
  end

  @doc """
  Read first and last record timestamps from a file without scanning
  every line in the middle. Returns `{first_ts_us, last_ts_us, record_count}`.

  Implementation streams the file end-to-end so the count is exact;
  the name preserves the contract documented in the spec, but the
  optimisation of "first + last only" is left as future work — for
  the rebuild path the count is required anyway.
  """
  @spec head_metadata(String.t()) :: {integer() | nil, integer() | nil, non_neg_integer()}
  def head_metadata(path) do
    stream(path, [])
    |> Enum.reduce({nil, nil, 0}, fn record, {first, _last, n} ->
      ts = Map.get(record, "ts_us")
      first = if is_nil(first), do: ts, else: first
      {first, ts, n + 1}
    end)
  end

  # ---------- Stream.resource callbacks ----------

  defp init_state(path, opts) do
    gz? = String.ends_with?(path, ".gz")

    case File.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        z =
          if gz? do
            z = :zlib.open()
            :zlib.inflateInit(z, 31)
            z
          else
            nil
          end

        %{
          fd: fd,
          z: z,
          buffer: "",
          eof?: false,
          filter: build_filter(opts),
          limit: Keyword.get(opts, :limit),
          matched: 0
        }

      {:error, reason} ->
        %{
          fd: nil,
          z: nil,
          buffer: "",
          eof?: true,
          filter: build_filter(opts),
          limit: nil,
          matched: 0,
          error: reason
        }
    end
  end

  defp next(%{limit: limit, matched: matched} = state)
       when is_integer(limit) and matched >= limit do
    {:halt, state}
  end

  defp next(%{fd: nil} = state), do: {:halt, state}

  defp next(state) do
    case extract_line(state) do
      {:line, line, state} ->
        case Codec.decode(line) do
          {:ok, record} ->
            if matches?(record, state.filter) do
              {[record], %{state | matched: state.matched + 1}}
            else
              next(state)
            end

          {:error, _} ->
            next(state)
        end

      {:eof, state} ->
        {:halt, state}
    end
  end

  defp cleanup(%{fd: fd, z: z}) do
    if z do
      try do
        :zlib.inflateEnd(z)
      catch
        _, _ -> :ok
      end

      try do
        :zlib.close(z)
      catch
        _, _ -> :ok
      end
    end

    if fd, do: :file.close(fd)
    :ok
  end

  # ---------- Line extraction ----------

  defp extract_line(state) do
    case :binary.split(state.buffer, "\n") do
      [line, rest] ->
        {:line, line, %{state | buffer: rest}}

      [_] ->
        if state.eof? do
          # Trailing data with no newline — treat as truncated.
          {:eof, state}
        else
          case fill(state) do
            {:ok, state} -> extract_line(state)
            {:eof, state} -> extract_line(%{state | eof?: true})
          end
        end
    end
  end

  defp fill(%{fd: fd, z: nil} = state) do
    case :file.read(fd, 65_536) do
      {:ok, chunk} -> {:ok, %{state | buffer: state.buffer <> chunk}}
      :eof -> {:eof, state}
      {:error, _} -> {:eof, state}
    end
  end

  defp fill(%{fd: fd, z: z} = state) do
    case :file.read(fd, 65_536) do
      {:ok, chunk} ->
        case :zlib.inflate(z, chunk) do
          out when is_list(out) ->
            decompressed = IO.iodata_to_binary(out)
            {:ok, %{state | buffer: state.buffer <> decompressed}}
        end

      :eof ->
        # Flush any remaining decompressor state.
        case safe_inflate(z, <<>>) do
          "" -> {:eof, state}
          tail -> {:ok, %{state | buffer: state.buffer <> tail, eof?: true}}
        end

      {:error, _} ->
        {:eof, state}
    end
  end

  defp safe_inflate(z, data) do
    try do
      out = :zlib.inflate(z, data)
      IO.iodata_to_binary(out)
    rescue
      _ -> ""
    catch
      _, _ -> ""
    end
  end

  # ---------- Filter ----------

  defp build_filter(opts) do
    %{
      source: Keyword.get(opts, :source),
      name: Keyword.get(opts, :name),
      since_us: Keyword.get(opts, :since_us),
      until_us: Keyword.get(opts, :until_us)
    }
  end

  defp matches?(record, filter) do
    field_match?(record, "source", filter.source) and
      field_match?(record, "name", filter.name) and
      ts_match?(record, filter.since_us, filter.until_us)
  end

  defp field_match?(_record, _key, nil), do: true
  defp field_match?(record, key, expected), do: Map.get(record, key) == expected

  defp ts_match?(record, since_us, until_us) do
    ts = Map.get(record, "ts_us")

    cond do
      not is_integer(ts) -> true
      is_integer(since_us) and ts < since_us -> false
      is_integer(until_us) and ts > until_us -> false
      true -> true
    end
  end
end
