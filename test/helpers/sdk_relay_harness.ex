defmodule FlezhaTest.Helper.SdkRelayHarness do
  @moduledoc """
  One SDK Relay in isolation — no controller, no real transport. Its
  secondary is a native `LocalStream` whose controller side the calling test
  owns, so every sink output and every record frame the Relay's Cells
  ship arrives at the test process as `{:flz_data, _id, payload_id, frame,
  metadata}`, in the order it was written. `frames/1` reads them back,
  `record/1` decodes a record frame, and the two record builders write the
  controller's on-disk framing for a restore.
  """

  import ExUnit.Assertions

  alias Flezha.SDK.{Counters, KeyGroup, Relay, Record, Topology}
  alias Flezha.SDK.Relay.Inbox
  alias Flezha.SDK.RecordFrame
  alias Flezha.SDK.Wire
  alias Flezha.SDK.LocalStream

  @doc """
  Start `op_module` as relay `op-1` (override with `:relay_id`,
  `:range`, `:runner`, `:credit`, `:hold_cap`).

  `:hold_cap` bounds the reconnect holds of this Relay and of every Cell
  it starts. A test that drives shedding states it here rather than writing
  `FLEZHA_RECONNECT_RING_CAPACITY`: that variable is process-wide and read at
  every construction, so setting it decides the cap for whatever else in the
  run happens to build a hold in the window.
  """
  def start_relay(op_module, opts \\ []) do
    {:ok, topo} = Topology.build(op_module)
    {sdk_sec, ctrl_sec} = own_secondary()

    counters = Counters.new()

    {:ok, pid} =
      Relay.Server.start_link(
        relay_id: Keyword.get(opts, :relay_id, "op-1"),
        topology: topo,
        range: Keyword.get(opts, :range, KeyGroup.decode(0, 0, 0, 0)),
        counters: counters,
        runner: Keyword.get(opts, :runner, self()),
        secondaries: [sdk_sec],
        credit: Keyword.get(opts, :credit),
        hold_cap: Keyword.get(opts, :hold_cap)
      )

    %{op: pid, counters: counters, ctrl_secondary: ctrl_sec, sdk_secondary: sdk_sec}
  end

  @doc """
  A fresh secondary whose controller side the calling process owns and reads:
  `{sdk_handle, controller_handle}`. What a runner's rebind hands the Relay.
  """
  def own_secondary do
    {sdk_sec, ctrl_sec} = LocalStream.open()
    :ok = Flezha.Transport.Stream.controlling_process(ctrl_sec, self())
    :ok = Flezha.Transport.Stream.set_active(ctrl_sec)
    {sdk_sec, ctrl_sec}
  end

  @doc """
  Close a secondary and wait for its stream process to be gone.

  `Flezha.Transport.Stream.close/1` returns as soon as the stream replies, one
  step before it terminates — so a write issued straight after can still find it
  alive and be delivered. Waiting for the exit is what makes "the wire is gone"
  true for the next write instead of merely likely.
  """
  def close_secondary(%LocalStream{pid: pid} = handle) do
    ref = Process.monitor(pid)
    :ok = Flezha.Transport.Stream.close(handle)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    :ok
  end

  @doc "Rebind `op` onto a fresh secondary this process owns; the controller handle."
  def rebind_secondary(op) do
    {sdk_sec, ctrl_sec} = own_secondary()
    send(op, {:secondaries, [sdk_sec]})
    ctrl_sec
  end

  @doc """
  The next `n` frames off the stream, in arrival order, as `{payload_id,
  frame, metadata}`. A processed-prefix marker is left in the mailbox for
  `marker/0`: it is the Relay's report, not something its Cells wrote.
  """
  def frames(n, acc \\ [])
  def frames(0, acc), do: Enum.reverse(acc)

  def frames(n, acc) do
    assert_receive {:flz_data, _id, payload_id, frame, meta}
                   when payload_id != "processed_prefix",
                   2_000

    frames(n - 1, [{payload_id, frame, meta} | acc])
  end

  @doc "The next processed-prefix marker as `{prefix, streams}`."
  def marker do
    assert_receive {:flz_data, _id, "processed_prefix", frame, _meta}, 2_000
    {:ok, %{seq: prefix, body: <<streams::16-big>>}} = Wire.decode_frame(frame)
    {prefix, streams}
  end

  @doc "A record frame as `{seq, change, decoded_frame}`."
  def record({"cell_state_change", frame, _meta}) do
    {:ok, %{seq: seq, body: body} = decoded} = Wire.decode_frame(frame)
    {:ok, change} = Record.decode_body(body)
    {seq, change, decoded}
  end

  @doc "`pid`, the Cell at `{relay_id, tasklet, key}`, is evicted: its delete frame lands, then it ends."
  def assert_evicted(pid, {relay_id, tasklet, key}) do
    ref = Process.monitor(pid)
    # A one-second TTL, then the next sweep a quarter of it later.
    assert_receive {:flz_data, _, "delete_cell_state", frame, _}, 3_000
    assert {:ok, {^relay_id, ^tasklet, ^key}} = Wire.decode_cell_id(frame)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000
  end

  @doc "Kill the Cell of `tasklet` untrappably through its `\"kill\"` input; the dead pid."
  def kill!(op, tasklet) do
    pid = cell!(op, tasklet)
    ref = Process.monitor(pid)
    Inbox.input(op, "in", "kill", %{})
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000
    pid
  end

  @doc """
  The issues a refused registration lists, as `{code, details}` in the order
  the refusal names them.
  """
  def refusal_issues({:error, %Flezha.SDK.Error{details: %{"issues" => issues}}}),
    do: Enum.map(issues, &{&1["code"], &1["details"]})

  @doc "The Cell of `tasklet` at `key`, which must exist."
  def cell!(op, tasklet, key \\ nil) do
    {:ok, pid} = Relay.Server.lookup(op, tasklet, key)
    pid
  end

  @doc "The controller's on-disk record, framed by the one encoder that writes it."
  def disk_record(seq, header, body), do: RecordFrame.encode_record(seq, header, body)

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
  def effect({"cell_effect_record", frame, _meta}) do
    {:ok, %{seq: seq, header: header, body: body}} = Wire.decode_frame(frame)
    {seq, Wire.decode_effect_record(header, body)}
  end

  @doc "A timer-ops record frame as `{seq, ops, decoded_frame}`."
  def timer_ops({"cell_state_change", frame, _meta}) do
    {:ok, %{seq: seq, body: body} = decoded} = Wire.decode_frame(frame)
    {:ok, ops} = Wire.decode_timer_ops(body)
    {seq, ops, decoded}
  end

  @doc """
  What a frame *is*, with no regard to where it sits: `{:change, seq, change}`,
  `{:timer_ops, seq, ops}`, `{:effect, seq, body}`, `{:delete, cell}` or
  `{:output, payload_id, value, metadata}`. A change record and a timer-ops record share the
  `cell_state_change` payload id and are told apart by the header's format
  byte, so this is the only way to classify one frame on its own.

  Order is then a question about positions in `frames/1`'s list and identity a
  question about these terms — which is what a reader of this stream must ask
  separately, because records and outputs are written by two processes.
  """
  def kind({"cell_state_change", frame, _meta} = f) do
    # Byte 9 of the 16-byte record header is the format id, as
    # `Wire.effect_record_header?/1` reads it.
    {:ok, %{header: <<_::binary-size(9), format, _::binary>>}} = Wire.decode_frame(frame)

    if format == Record.timer_ops_format_id() do
      {seq, ops, _} = timer_ops(f)
      {:timer_ops, seq, ops}
    else
      {seq, change, _} = record(f)
      {:change, seq, change}
    end
  end

  def kind({"cell_effect_record", _frame, _meta} = f) do
    {seq, body} = effect(f)
    {:effect, seq, body}
  end

  def kind({"delete_cell_state", identity, _meta}) do
    {:ok, cell} = Wire.decode_cell_id(identity)
    {:delete, cell}
  end

  # Everything else on this stream is a plugin output, whose payload id is the
  # relay's. The other record payload ids are named so one arriving where a
  # test does not expect it raises here instead of passing as an output.
  def kind({payload_id, value, meta}) when payload_id != "cell_state_snapshot",
    do: {:output, payload_id, value, meta}
end
