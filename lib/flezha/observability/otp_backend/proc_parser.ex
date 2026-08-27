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
    fields = fields(text)

    {:ok, %{vm_rss_bytes: kib_field(fields, "VmRSS"), fd_size: int_field(fields, "FDSize")}}
  end

  @doc """
  Parse `/proc/cpuinfo`. Returns `:model` (the first `model name`, which is
  uniform across cores on every machine this runs on) and `:cores` (how many
  processor entries the file lists).
  """
  @spec parse_cpuinfo(String.t()) :: {:ok, map()} | {:error, :unparseable}
  def parse_cpuinfo(text) when is_binary(text) do
    model = fields(text)["model name"]
    # Counted rather than read from the field map, which keeps one entry per
    # key: the file repeats every field once per core.
    cores = Enum.count(String.split(text, "\n"), &(key_of(&1) == "processor"))

    if model || cores > 0 do
      {:ok, %{model: model, cores: cores}}
    else
      {:error, :unparseable}
    end
  end

  @doc """
  Parse `/proc/meminfo`. Returns `:mem_total_bytes` from the `MemTotal` line,
  which the file reports in kibibytes.
  """
  @spec parse_meminfo(String.t()) :: {:ok, map()} | {:error, :unparseable}
  def parse_meminfo(text) when is_binary(text) do
    case kib_field(fields(text), "MemTotal") do
      nil -> {:error, :unparseable}
      total -> {:ok, %{mem_total_bytes: total}}
    end
  end

  # Every `/proc` file here but `loadavg` is `key: value` lines. First entry
  # wins, because a repeated key (`/proc/cpuinfo` repeats every field per core)
  # names the same value each time and the first is the one a reader expects.
  defp fields(text) do
    text
    |> String.split("\n")
    |> Enum.reduce(%{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [key, value] -> Map.put_new(acc, String.trim(key), String.trim(value))
        _ -> acc
      end
    end)
  end

  defp key_of(line) do
    case String.split(line, ":", parts: 2) do
      [key, _] -> String.trim(key)
      _ -> nil
    end
  end

  # A leading integer, or `nil` when the field is absent or unparseable.
  defp int_field(fields, key) do
    with value when is_binary(value) <- Map.get(fields, key),
         {n, _rest} <- Integer.parse(value) do
      n
    else
      _ -> nil
    end
  end

  # `/proc` reports memory in kibibytes; callers want bytes.
  defp kib_field(fields, key) do
    case int_field(fields, key) do
      nil -> nil
      kib -> kib * 1024
    end
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
