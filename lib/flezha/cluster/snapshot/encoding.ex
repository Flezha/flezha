defmodule Flezha.Cluster.Snapshot.Encoding do
  @moduledoc """
  ETF envelope for cluster snapshot files: `{:flezha_snapshot, version, payload}`.

  `encode/1` wraps a payload term in the current schema version and produces a
  compressed ETF binary. `decode/1` is the only place that pattern-matches the
  version tag and is the single source of truth for "is this file readable by
  this agent." A version mismatch is a hard error — see `cluster-snapshot-restore`'s
  spec on schema-version handling.
  """

  @version 1

  @spec version() :: pos_integer()
  def version, do: @version

  @spec encode(map()) :: binary()
  def encode(payload) when is_map(payload) do
    :erlang.term_to_binary({:flezha_snapshot, @version, payload}, [:compressed])
  end

  @spec decode(binary()) ::
          {:ok, map()}
          | {:error, {:unsupported_version, integer(), [pos_integer()]}}
          | {:error, :invalid_envelope}
          | {:error, {:decode_failed, term()}}
  def decode(bin) when is_binary(bin) do
    try do
      case :erlang.binary_to_term(bin) do
        {:flezha_snapshot, @version, payload} when is_map(payload) ->
          {:ok, payload}

        {:flezha_snapshot, other, _payload} when is_integer(other) ->
          {:error, {:unsupported_version, other, [@version]}}

        _other ->
          {:error, :invalid_envelope}
      end
    rescue
      e -> {:error, {:decode_failed, Exception.message(e)}}
    catch
      kind, reason -> {:error, {:decode_failed, {kind, reason}}}
    end
  end
end
