defmodule FlezhaTest.Helper.CallHarness do
  @moduledoc """
  What the relay-call Overwatch tests share: every hand-off to the call
  delivery, every Response channel input and every outbox report reaches the
  test process, and a call end's output as the runner stamps it.
  """

  import ExUnit.Assertions
  import Mimic

  alias Flezha.Process.CallDelivery
  alias Flezha.State.{Outbox, WriterRegistry}
  alias FlezhaTest.Helper.OverwatchHost, as: Host
  alias FlezhaTest.Helper.Telemetry

  @cell {"gateway", "account", "k"}
  @events [:answered, :timeout, :answer_dropped, :refused, :recovered_unanswered]

  @doc """
  Stub the stream, the delivery and the outbox's writers so each reports to the
  test as `{:channel_got, id, payload}`, `{:delivery_request, ...}`,
  `{:delivery_answer, ...}` and the writer's own casts; attach the call events
  as `{:call_event, event, metadata}`. Input logs stay real.
  """
  def install do
    test_pid = self()

    Telemetry.capture(for(e <- @events, do: [:flezha, :call, e]),
      as: fn [:flezha, :call, event], _measurements, metadata ->
        {:call_event, event, metadata}
      end
    )

    stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, id, payload ->
      send(test_pid, {:channel_got, id, payload})
      :ok
    end)

    stub(CallDelivery, :request, fn ns, caller, request ->
      send(test_pid, {:delivery_request, ns, caller, request})
      :ok
    end)

    stub(CallDelivery, :answer, fn ns, callee, call, outcome, metadata ->
      send(test_pid, {:delivery_answer, ns, callee, call, outcome, metadata})
      :ok
    end)

    stub(WriterRegistry, :lookup, fn
      {_class, :input_log} = log -> Mimic.call_original(WriterRegistry, :lookup, [log])
      _owner -> {:ok, test_pid}
    end)

    :ok
  end

  def now, do: System.system_time(:millisecond)

  @doc """
  A call end's output as the runner stamped it; `durable` adds the outbox
  identity and the confirmation token a durable caller's output carries.
  """
  def request_metadata(deadline_at, durable \\ true) do
    stamps = %{
      "channel" => "ask",
      "flz.call" => "factory:agent",
      "flz.call_response" => "ask.response",
      "flz.deadline_at" => Integer.to_string(deadline_at),
      "flz.key" => "anna-42"
    }

    if durable,
      do:
        Map.merge(stamps, %{
          "flz.cell" => "gateway/account/k",
          "flz.seq" => "4.0",
          "flz.epoch" => "0",
          "flz.ack" => Outbox.token(@cell, {:cursor, 4})
        }),
      else: stamps
  end

  @doc "One outbox dispatch of the call output, as the writer casts it."
  def dispatch(host, message, metadata),
    do: GenServer.cast(host, {:io_out_batch, [{message, metadata, %{owner: @cell, key: 4}}]})

  @doc "An Overwatch whose input is durable, as a caller of a durable pair is."
  def durable_caller(inst) do
    state = Host.state(inst, durable_steps: true, channel: :fake_channel)
    ExUnit.Callbacks.start_supervised!({Host, state})
  end

  def ok_outcome(value, created \\ false),
    do: %{result: {:ok, value}, target: "anna-42", created: created}

  @doc "The Response the caller's Response channel received, decoded, with its metadata."
  def response(inst) do
    assert_receive {:channel_got, ^inst, {envelope, %{"channel" => "ask.response"} = metadata}},
                   2_000

    {Jason.decode!(envelope), metadata}
  end
end
