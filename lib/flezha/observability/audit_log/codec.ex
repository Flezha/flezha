defmodule Flezha.Observability.AuditLog.Codec do
  @moduledoc """
  Encode / decode an audit log record.

  Each record is a single UTF-8 JSON object on its own line, terminated
  by `\\n`. Top-level field order is stable:
  `ts_us, ts_iso, source, name, measurements, metadata, caller`.

  `caller` is denormalised from `metadata.caller_source`,
  `metadata.principal`, and `metadata.request_id` so a reader does not
  need to know the metadata convention to identify the actor. A
  missing `:principal` renders as JSON `null`, not absent.

  `:ts` on a `Flezha.Observability.Signal` is `System.monotonic_time(:microsecond)`;
  the `:ts_iso` field is its wall-clock time, `Signal.wall_us/1`.
  """

  alias Flezha.Observability.Signal

  @doc """
  Encode a `Signal.t()` to a binary line ending in `\\n`.
  """
  @spec encode(Signal.t()) :: binary()
  def encode(%Signal{} = signal) do
    ts_us = signal.ts
    ts_iso = iso_from_monotonic(ts_us)
    caller = build_caller(signal.metadata)

    namespace = Map.get(signal.metadata, :namespace) || "_system"

    ordered = [
      {"ts_us", ts_us},
      {"ts_iso", ts_iso},
      {"source", Atom.to_string(signal.source)},
      {"name", Atom.to_string(signal.name)},
      {"measurements", Signal.metadata_json(signal.measurements)},
      {"metadata", Signal.metadata_json(signal.metadata)},
      {"caller", caller},
      {"namespace", namespace}
    ]

    inner =
      ordered
      |> Enum.map_join(",", fn {k, v} ->
        Jason.encode!(k) <> ":" <> Jason.encode!(v)
      end)

    "{" <> inner <> "}\n"
  end

  @doc """
  Decode a binary line (with or without trailing `\\n`) into a map
  with string keys. Returns `{:ok, map}` or `{:error, reason}`.
  """
  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(line) when is_binary(line) do
    trimmed = String.trim_trailing(line, "\n")

    case Jason.decode(trimmed) do
      {:ok, %{} = map} -> {:ok, map}
      {:ok, _} -> {:error, :not_an_object}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Map a monotonic microsecond timestamp to ISO-8601 UTC with
  millisecond precision, through `Signal.wall_us/1`.
  """
  @spec iso_from_monotonic(integer()) :: String.t()
  def iso_from_monotonic(ts_us) when is_integer(ts_us) do
    {:ok, dt} = DateTime.from_unix(Signal.wall_us(ts_us), :microsecond)

    ms = div(elem(dt.microsecond, 0), 1_000)
    dt = %{dt | microsecond: {ms * 1_000, 3}}
    iso = DateTime.to_iso8601(dt)

    # DateTime.to_iso8601/1 emits `+00:00` for UTC; the spec requires `Z`.
    String.replace(iso, "+00:00", "Z")
  end

  defp build_caller(metadata) when is_map(metadata) do
    # When `:authn_authz, :enabled` is false the audit log records
    # the caller_source atom in the principal column (preserving
    # pre-change behaviour). Once enabled, the verified principal
    # bound by the authentication layer wins.
    principal =
      cond do
        Flezha.AuthZ.enabled?() -> Map.get(metadata, :principal)
        true -> Map.get(metadata, :principal) || Map.get(metadata, :caller_source)
      end

    %{
      "source" => atom_or_string(Map.get(metadata, :caller_source)),
      "principal" => atom_or_string(principal),
      "request_id" => Map.get(metadata, :request_id)
    }
  end

  defp atom_or_string(nil), do: nil
  defp atom_or_string(a) when is_atom(a), do: Atom.to_string(a)
  defp atom_or_string(other), do: other
end
