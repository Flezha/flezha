defmodule Mix.Tasks.Bench.Inputs do
  @moduledoc """
  What `mix bench` sends and how: the options that shape an input, checked
  before the controller boots, the one builder every send path uses, and the
  burst and paced senders.

  An input is the shape's wire form around a body. The body is the
  `<seq>,<send_ns>` stamp under `--latency` (which `Tally.parse_stamp/1` reads
  back from the output) and `test_msg_<relay>_<n>` otherwise, padded to
  `--payload-bytes` when set. The payload range a run reports is taken from
  this builder at its extreme indices, so what the header states is what the
  send loop builds.
  """

  alias Flezha.Plugin.Host
  alias Mix.Tasks.Bench.Report
  alias Mix.Tasks.Bench.Tally

  @type t :: %{
          via: :cast | :host_inject,
          latency: boolean(),
          rate: pos_integer() | nil,
          pad: {pos_integer(), binary()} | nil,
          payload: {non_neg_integer(), non_neg_integer()}
        }

  @doc """
  The run's input settings from its parsed options, or a `Mix.Error` naming
  the combination that cannot run. `dims` is `{relays, messages_per_relay}`.
  """
  @spec parse(keyword(), atom(), atom(), {pos_integer(), non_neg_integer()}, non_neg_integer()) ::
          t()
  def parse(opts, shape, via, {num_relays, messages_per_relay}, warmup) do
    rate = opts[:rate]
    bytes = opts[:payload_bytes]
    refuse(rate, bytes, shape, via)

    dims = {num_relays, messages_per_relay, warmup}
    inputs = %{via: via, latency: opts[:latency] || false, rate: rate, pad: nil}
    {_, unpadded} = payload_range(shape, inputs, dims)

    if bytes != nil and bytes < unpadded do
      Mix.raise(
        "--payload-bytes #{bytes} is below this run's unpadded input of #{unpadded} bytes; " <>
          "use at least #{unpadded}"
      )
    end

    inputs = %{inputs | pad: bytes && {bytes, String.duplicate("x", bytes)}}
    Map.put(inputs, :payload, payload_range(shape, inputs, dims))
  end

  defp refuse(rate, _bytes, _shape, _via) when rate != nil and rate <= 0,
    do: Mix.raise("--rate must be a positive integer, got #{rate}")

  defp refuse(rate, _bytes, _shape, via) when rate != nil and via != :cast,
    do:
      Mix.raise(
        "--rate needs --via cast: host_inject waits for each input's verdict on one " <>
          "caller per relay, so a single paced sender cannot drive it"
      )

  defp refuse(rate, _bytes, :cell, _via) when rate != nil,
    do: Mix.raise("--rate does not apply to --shape cell, whose fixed sweep stays comparable")

  defp refuse(_rate, bytes, :sleeping, _via) when bytes != nil,
    do: Mix.raise("--payload-bytes does not apply to --shape sleeping: its input is an integer")

  defp refuse(_rate, bytes, _shape, _via) when bytes != nil and bytes <= 0,
    do: Mix.raise("--payload-bytes must be a positive integer, got #{bytes}")

  defp refuse(_rate, _bytes, _shape, _via), do: :ok

  # The shortest input is the first relay's first, the longest the last relay's
  # last: every variable part of an input grows with its indices.
  defp payload_range(shape, inputs, {num_relays, messages_per_relay, warmup}) do
    [
      build(inputs, shape, relay_id(1), 1, 0),
      build(
        inputs,
        shape,
        relay_id(num_relays),
        max(warmup, messages_per_relay),
        num_relays * (warmup + messages_per_relay) - 1
      )
    ]
    |> Enum.map(&byte_size/1)
    |> Enum.min_max()
  end

  @doc "The run-block rows these settings add to the bench header."
  @spec header_rows(t()) :: [{String.t(), String.t()}]
  def header_rows(%{payload: {min, max}} = inputs) do
    [
      {"payload", "#{min}–#{max} B"},
      {"send", if(inputs.rate, do: "paced, #{Report.rate(inputs.rate)}/s", else: "burst")},
      {"latency", if(inputs.latency, do: "on", else: "off")}
    ]
  end

  @doc "The id of the bench's relay `i`; `suffix` names a second relay set."
  @spec relay_id(pos_integer(), String.t()) :: String.t()
  def relay_id(i, suffix \\ ""), do: "flow_test_#{i}#{suffix}"

  @doc "Input `msg_num` of `relay_id`, as seq `seq` of the run."
  @spec build(map(), atom(), String.t(), pos_integer(), non_neg_integer()) :: binary()
  def build(%{latency: latency, pad: pad}, shape, relay_id, msg_num, seq) do
    body =
      if latency,
        do: "#{seq},#{System.monotonic_time(:nanosecond)}",
        else: "test_msg_#{relay_id}_#{msg_num}"

    shape |> wire(relay_id, msg_num, body) |> pad(pad, latency)
  end

  @doc """
  Send each relay its inputs in turn, through the run's `--via`. Seqs run from
  `base`, relay by relay: relay `r`'s input `k` is `base + r * messages_per_relay + k - 1`.
  """
  @spec send_burst(t(), [{String.t(), map()}], atom(), non_neg_integer(), non_neg_integer()) ::
          :ok
  def send_burst(inputs, relay_infos, shape, messages_per_relay, base) do
    send_burst(inputs.via, relay_infos, shape, messages_per_relay, base, inputs)
  end

  defp send_burst(:cast, relay_infos, shape, messages_per_relay, base, inputs) do
    relay_infos
    |> Enum.with_index()
    |> Enum.each(fn {{relay_id, %{pid: relay_pid}}, r} ->
      for msg_num <- 1..messages_per_relay//1 do
        seq = base + r * messages_per_relay + msg_num - 1

        GenServer.cast(
          relay_pid,
          {:io_in, build(inputs, shape, relay_id, msg_num, seq), %{}}
        )
      end
    end)
  end

  defp send_burst(:host_inject, relay_infos, shape, messages_per_relay, base, inputs) do
    Flezha.Parallel.each(
      Enum.with_index(relay_infos),
      fn {{relay_id, %{pid: relay_pid}}, r} ->
        for msg_num <- 1..messages_per_relay//1 do
          # Unique across the warmup and the measured pass, which repeat `msg_num`.
          metadata = %{"flz.msg_id" => "#{relay_id}:#{System.unique_integer([:positive])}"}
          seq = base + r * messages_per_relay + msg_num - 1
          input = build(inputs, shape, relay_id, msg_num, seq)

          with {:error, reason} <- Host.inject(relay_pid, input, metadata, :verdict),
               do: Mix.raise("#{relay_id} refused an input: #{inspect(reason)}")
        end
      end,
      max_concurrency: max(length(relay_infos), 1)
    )
  end

  @doc """
  Send the run's measured inputs: paced when it has a `--rate`, else as one
  burst.
  """
  @spec send(t(), [{String.t(), map()}], atom(), non_neg_integer(), non_neg_integer()) :: :ok
  def send(%{rate: nil} = inputs, relay_infos, shape, messages_per_relay, base),
    do: send_burst(inputs, relay_infos, shape, messages_per_relay, base)

  def send(inputs, relay_infos, shape, messages_per_relay, base),
    do: send_paced(inputs, relay_infos, shape, messages_per_relay, base)

  # Send at `inputs.rate` inputs/s in total, round-robin over the relays: input
  # `i` goes to relay `rem(i, n)` as seq `base + i`.
  defp send_paced(inputs, relay_infos, shape, messages_per_relay, base) do
    relays = List.to_tuple(relay_infos)
    total = tuple_size(relays) * messages_per_relay
    paced_loop(relays, shape, inputs, total, base, System.monotonic_time(:nanosecond), 0)
  end

  defp paced_loop(_relays, _shape, _inputs, total, _base, _t0, issued) when issued >= total,
    do: :ok

  defp paced_loop(relays, shape, inputs, total, base, t0, issued) do
    n = tuple_size(relays)
    due = min(Tally.due(t0, inputs.rate, issued), total - issued)

    if due == 0, do: Process.sleep(1)

    for i <- issued..(issued + due - 1)//1 do
      {relay_id, %{pid: relay_pid}} = elem(relays, rem(i, n))
      input = build(inputs, shape, relay_id, div(i, n) + 1, base + i)
      GenServer.cast(relay_pid, {:io_in, input, %{}})
    end

    paced_loop(relays, shape, inputs, total, base, t0, issued + due)
  end

  # One sender sleep tick plus the last output's own flight.
  @pace_slack_us 5_000

  @doc """
  Warn when a paced run of `total` inputs ended past its schedule: its sender
  or its drain fell behind, so its outputs queued and its latency is that of
  saturation. A burst has no schedule to keep.
  """
  @spec check_pace(t(), non_neg_integer(), pos_integer()) :: :ok
  def check_pace(%{rate: nil}, _total, _end_to_end_us), do: :ok

  def check_pace(%{rate: rate}, total, end_to_end_us) do
    schedule_us = div(total * 1_000_000, rate)

    if end_to_end_us > schedule_us + max(div(schedule_us, 20), @pace_slack_us) do
      Report.warn(
        "the run reached #{Report.rate(Report.per_sec(total, end_to_end_us))}/s end to end of " <>
          "the #{Report.rate(rate)}/s asked; its latency includes queueing at saturation, " <>
          "not latency at the asked rate"
      )
    end

    :ok
  end

  # A stamped input pads after a `,`, which the stamp parser reads as a third field.
  defp pad(input, nil, _stamped), do: input

  defp pad(input, {bytes, filler}, stamped) do
    case bytes - byte_size(input) do
      short when short <= 0 -> input
      short when stamped -> input <> "," <> binary_part(filler, 0, short - 1)
      short -> input <> binary_part(filler, 0, short)
    end
  end

  # Per-shape wire input. The keyed shapes prefix a bounded key (`k0`..`k7`)
  # so a steady burst exercises 8 Cells per relay without tripping the
  # dynamic soft cap; `sleeping_counter` folds the value as an integer delta.
  defp wire(:sleeping, _relay_id, msg_num, _body), do: "k#{rem(msg_num, 8)}:1"

  defp wire(shape, _relay_id, msg_num, body) when shape in [:dynamic, :tier],
    do: "k#{rem(msg_num, 8)}:#{body}"

  defp wire(:send, relay_id, _msg_num, body), do: "#{relay_id}_sink:#{body}"
  defp wire(_shape, _relay_id, _msg_num, body), do: body
end
