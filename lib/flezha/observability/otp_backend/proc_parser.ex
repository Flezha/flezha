defmodule Flezha.Observability.OtpBackend.ProcParser do
  @moduledoc """
  Pure parsers for the small subset of Linux `/proc` files the
  `:host` source consumes. No I/O — callers read the bytes and hand
  them in. Keeps the parsers cheap to unit-test against fixture
  strings.
  """

  @doc """
  Parse `/proc/self/stat`. Returns a map carrying `:utime`, `:stime`,
  `:num_threads`, `:rss_pages`, and `:starttime` (the only fields the
  `:host` source needs today). Returns `{:error, :unparseable}` if the
  line cannot be tokenised as expected.
  """
  @spec parse_stat(String.t()) :: {:ok, map()} | {:error, :unparseable}
  def parse_stat(line) when is_binary(line) do
    # The 2nd field (comm) is parenthesised and may contain spaces;
    # we anchor on the rightmost `)` so trailing fields tokenise
    # cleanly.
    case String.split(line, ")", parts: 2) do
      [_left, rest] ->
        case String.split(String.trim(rest)) do
          fields when length(fields) >= 22 ->
            # Indices are 0-based from `man proc` field 3 (state) onward,
            # since we dropped the first two fields ahead of the split.
            # That makes state=fields[0]; utime=fields[11]; stime=12;
            # num_threads=17; starttime=19; rss_pages=21.
            with {utime, ""} <- Integer.parse(Enum.at(fields, 11)),
                 {stime, ""} <- Integer.parse(Enum.at(fields, 12)),
                 {nthreads, ""} <- Integer.parse(Enum.at(fields, 17)),
                 {starttime, ""} <- Integer.parse(Enum.at(fields, 19)),
                 {rss_pages, ""} <- Integer.parse(Enum.at(fields, 21)) do
              {:ok,
               %{
                 utime: utime,
                 stime: stime,
                 num_threads: nthreads,
                 starttime: starttime,
                 rss_pages: rss_pages
               }}
            else
              _ -> {:error, :unparseable}
            end

          _ ->
            {:error, :unparseable}
        end

      _ ->
        {:error, :unparseable}
    end
  end

  @doc """
  Parse the relevant `Name: value` lines from `/proc/self/status`.
  Returns a map with `:vm_rss_bytes` and `:fd_size` (the kernel's
  current FD-table capacity is the closest proxy for an "open FD count"
  we can get from `/status` alone; the source can also separately list
  `/proc/self/fd/` for the exact count).
  """
  @spec parse_status(String.t()) :: {:ok, map()} | {:error, :unparseable}
  def parse_status(text) when is_binary(text) do
    result =
      text
      |> String.split("\n")
      |> Enum.reduce(%{}, fn line, acc ->
        case String.split(line, ":", parts: 2) do
          [key, value] -> Map.put(acc, String.trim(key), String.trim(value))
          _ -> acc
        end
      end)

    vm_rss_bytes =
      case Map.get(result, "VmRSS") do
        nil ->
          nil

        v ->
          case Integer.parse(v) do
            {kb, _rest} -> kb * 1024
            _ -> nil
          end
      end

    fd_size =
      case Map.get(result, "FDSize") do
        nil ->
          nil

        v ->
          case Integer.parse(v) do
            {n, _} -> n
            _ -> nil
          end
      end

    {:ok, %{vm_rss_bytes: vm_rss_bytes, fd_size: fd_size}}
  end

  @doc """
  Parse `/proc/loadavg`. Returns a map with `:load_1m`, `:load_5m`,
  `:load_15m` as floats.
  """
  @spec parse_loadavg(String.t()) :: {:ok, map()} | {:error, :unparseable}
  def parse_loadavg(line) when is_binary(line) do
    fields = String.split(String.trim(line))

    with [a, b, c | _] <- fields,
         {l1, _} <- Float.parse(a),
         {l5, _} <- Float.parse(b),
         {l15, _} <- Float.parse(c) do
      {:ok, %{load_1m: l1, load_5m: l5, load_15m: l15}}
    else
      _ -> {:error, :unparseable}
    end
  end
end
