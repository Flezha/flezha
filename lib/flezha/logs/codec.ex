defmodule Flezha.Logs.Codec do
  @moduledoc """
  Encode and decode one log record.

  Each record is a single UTF-8 JSON object on its own line, terminated by
  `\\n`, its fields in a stable order: `ts_us`, `ts_iso`, then the kind's own
  fields in the order its `encode` gives them, then `node`, the controller
  that wrote it. `ts_us` is wall-clock microseconds and `ts_iso` the same
  instant to the millisecond, so records of several controllers order
  together.

  A record written before `node` was added carried the writer's monotonic
  time in `ts_us`; `order_us/1` reads such a record's time from its `ts_iso`.
  """

  alias Flezha.Logs.Kind

  @doc "Encode `input` as a record line of `kind`, ending in `\\n`."
  @spec encode(Kind.t(), term()) :: binary()
  def encode(%Kind{encode: encode}, input) do
    {ts_us, fields} = encode.(input)

    ordered =
      [{"ts_us", ts_us}, {"ts_iso", iso(ts_us)}] ++
        fields ++ [{"node", Atom.to_string(node())}]

    inner =
      Enum.map_join(ordered, ",", fn {k, v} -> Jason.encode!(k) <> ":" <> Jason.encode!(v) end)

    "{" <> inner <> "}\n"
  end

  @doc """
  Decode a binary line (with or without trailing `\\n`) into a map
  with string keys. Returns `{:ok, map}` or `{:error, reason}`.
  """
  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(line) when is_binary(line) do
    case Jason.decode(String.trim_trailing(line, "\n")) do
      {:ok, %{} = map} -> {:ok, map}
      {:ok, _} -> {:error, :not_an_object}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  The wall-clock microseconds a record is ordered by: its `ts_us` when it
  names its `node`, else its `ts_iso`.
  """
  @spec order_us(map()) :: integer() | nil
  def order_us(%{"node" => _, "ts_us" => ts_us}) when is_integer(ts_us), do: ts_us

  def order_us(%{"ts_iso" => iso}) when is_binary(iso) do
    case DateTime.from_iso8601(iso) do
      {:ok, dt, _offset} -> DateTime.to_unix(dt, :microsecond)
      _ -> nil
    end
  end

  def order_us(_record), do: nil

  @doc "Wall-clock microseconds as ISO-8601 UTC with millisecond precision."
  @spec iso(integer()) :: String.t()
  def iso(ts_us) when is_integer(ts_us) do
    ts_us
    |> div(1_000)
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_iso8601()
  end
end
