defmodule FlezhaTest.Helper.SdkOperativeHarness do
  @moduledoc """
  One SDK Operative in isolation — no controller, no real transport. Its
  secondary is a native `Local.Stream` whose controller side the calling test
  owns, so every sink output and every record frame the Operative's Entities
  ship arrives at the test process as `{:flz_data, _id, payload_id, frame,
  metadata}`, in the order it was written. `frames/1` reads them back,
  `record/1` decodes a record frame, and the two record builders write the
  controller's on-disk framing for a restore.
  """

  import ExUnit.Assertions

  alias Flezha.SDK.{Counters, KeyGroup, Operative, Record, Topology}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.State.Wire
  alias Flezha.Transport.Local

  @doc "Start `op_module` as operative `op-1` (override with `:operative_id`, `:range`, `:runner`, `:credit`)."
  def start_operative(op_module, opts \\ []) do
    {:ok, topo} = Topology.build(op_module)
    {sdk_sec, ctrl_sec} = Local.Stream.open()
    :ok = Flezha.Transport.Stream.controlling_process(ctrl_sec, self())
    :ok = Flezha.Transport.Stream.set_active(ctrl_sec)

    counters = Counters.new()

    {:ok, pid} =
      Operative.start_link(
        operative_id: Keyword.get(opts, :operative_id, "op-1"),
        topology: topo,
        range: Keyword.get(opts, :range, KeyGroup.decode(0, 0, 0, 0)),
        counters: counters,
        runner: Keyword.get(opts, :runner, self()),
        secondaries: [sdk_sec],
        credit: Keyword.get(opts, :credit)
      )

    %{op: pid, counters: counters, ctrl_secondary: ctrl_sec, sdk_secondary: sdk_sec}
  end

  @doc "The next `n` frames off the stream, in arrival order, as `{payload_id, frame, metadata}`."
  def frames(n, acc \\ [])
  def frames(0, acc), do: Enum.reverse(acc)

  def frames(n, acc) do
    assert_receive {:flz_data, _id, payload_id, frame, meta}, 2_000
    frames(n - 1, [{payload_id, frame, meta} | acc])
  end

  @doc "A record frame as `{seq, change, decoded_frame}`."
  def record({"entity_state_change", frame, _meta}) do
    {:ok, %{seq: seq, body: body} = decoded} = Wire.decode_frame(frame)
    {:ok, change} = Record.decode_body(body)
    {seq, change, decoded}
  end

  @doc "`pid`, the Entity at `{operative_id, procedure, key}`, is evicted: its delete frame lands, then it ends."
  def assert_evicted(pid, {operative_id, procedure, key}) do
    ref = Process.monitor(pid)
    assert_receive {:flz_data, _, "delete_entity_state", frame, _}, 2_000
    assert {:ok, {^operative_id, ^procedure, ^key}} = Wire.decode_entity_id(frame)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
  end

  @doc "Kill the Entity of `procedure` untrappably through its `\"kill\"` input; the dead pid."
  def kill!(op, procedure) do
    pid = entity!(op, procedure)
    ref = Process.monitor(pid)
    Inbox.input(op, "in", "kill", %{})
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
    pid
  end

  @doc "The Entity of `procedure` at `key`, which must exist."
  def entity!(op, procedure, key \\ nil) do
    {:ok, pid} = Operative.lookup(op, procedure, key)
    pid
  end

  @doc "The controller's on-disk record: `[seq][len][header][body][crc over header <> body]`."
  def disk_record(seq, header, body) do
    <<seq::64-big, byte_size(header) + byte_size(body)::32-big, header::binary, body::binary,
      :erlang.crc32(header <> body)::32-big>>
  end

  @doc "A change record this SDK would have written, at `schema`."
  def change_record(seq, change, schema \\ 1) do
    body = Record.encode_body(change)
    disk_record(seq, Record.header(body, schema), body)
  end

  @doc "A `format_id = 3` timer-ops record, as any SDK writes it, at `schema`."
  def timer_ops_record(seq, ops, schema \\ 1) do
    body = Wire.encode_timer_ops(ops)
    disk_record(seq, Record.header(body, schema, Record.timer_ops_format_id()), body)
  end

  @doc "A `format_id = 2` effect record, as any SDK writes it, at `schema`."
  def effect_record(seq, body_map, schema \\ 1) do
    body = Wire.encode_effect_body(body_map)
    disk_record(seq, Record.header(body, schema, Record.effect_record_format_id()), body)
  end

  @doc "An effect record frame as `{seq, decoded_body}`; nil when the frame is not one."
  def effect({"entity_effect_record", frame, _meta}) do
    {:ok, %{seq: seq, header: header, body: body}} = Wire.decode_frame(frame)
    {seq, Wire.decode_effect_record(header, body)}
  end

  @doc "A timer-ops record frame as `{seq, ops, decoded_frame}`."
  def timer_ops({"entity_state_change", frame, _meta}) do
    {:ok, %{seq: seq, body: body} = decoded} = Wire.decode_frame(frame)
    {:ok, ops} = Wire.decode_timer_ops(body)
    {seq, ops, decoded}
  end
end
