defmodule FlezhaTest.Kafka.ProducerFakes do
  @moduledoc """
  A scripted stand-in for the `KafkaEx.Client` a `ProducerWorker` sends through.

  The real client answers by message, so these do too: a send returns a dispatch
  handle at once and the outcome reaches the worker's own mailbox later.
  `script_send_fn/2` pops one entry per send from an Agent-held list —
  `{:return, value}`, `{:return_fn, fun}`, or, once the list runs out,
  `:never` — where a value may be `:never` (a dispatch that is never answered,
  so the batch stays in flight) or `{:delayed, value, ms}` (answered after
  `ms`). `relay_send_fn/1` hands every dispatch to the test as
  `{:dispatched, ref, request}` instead, so a test can answer any batch in any
  order with `answer/3`.
  """

  def script_send_fn(test_pid, script_pid) do
    fn worker, request, node_selector ->
      send(test_pid, {:send_called, request, node_selector})

      scripted =
        Agent.get_and_update(script_pid, fn
          [] -> {{:return, :never}, []}
          [head | tail] -> {head, tail}
        end)

      case scripted do
        {:return, value} -> respond(value)
        {:return_fn, fun} -> respond(fun.(worker, request, node_selector))
      end
    end
  end

  def relay_send_fn(test_pid) do
    fn _worker, request, _node_selector ->
      ref = make_ref()
      send(test_pid, {:dispatched, ref, request})
      {:ok, ref}
    end
  end

  def answer(worker, ref, result), do: send(worker, {:kafka_ex_response, ref, result})

  def init_producer_id_ok(producer_id \\ 7777, epoch \\ 0) do
    fn _worker, _timeout -> {:ok, %{producer_id: producer_id, producer_epoch: epoch}} end
  end

  def init_producer_id_fail(reason), do: fn _worker, _timeout -> {:error, reason} end

  def metadata_refresh_ok, do: fn _worker, _timeout -> :ok end

  def create_topic_ok, do: fn _worker, _topic, _opts, _timeout -> :ok end

  def create_topic_already_exists,
    do: fn _worker, _topic, _opts, _timeout -> {:error, :topic_already_exists} end

  def ok_response(topic, partition, base_offset \\ 100),
    do: produce_response(topic, partition, 0, base_offset)

  def err_response(topic, partition, error_code),
    do: produce_response(topic, partition, error_code, -1)

  defp produce_response(topic, partition, error_code, base_offset) do
    {:ok,
     %Kayrock.Produce.V5.Response{
       responses: [
         %{
           topic: topic,
           partition_responses: [
             %{
               partition: partition,
               error_code: error_code,
               base_offset: base_offset,
               log_append_time: -1,
               log_start_offset: -1
             }
           ]
         }
       ],
       throttle_time_ms: 0
     }}
  end

  # The send runs in the worker, so the scripted outcome goes to its mailbox.
  defp respond(:never), do: {:ok, make_ref()}

  defp respond({:delayed, value, delay_ms}) do
    ref = make_ref()
    Process.send_after(self(), {:kafka_ex_response, ref, value}, delay_ms)

    {:ok, ref}
  end

  defp respond(value) do
    ref = make_ref()
    send(self(), {:kafka_ex_response, ref, value})
    {:ok, ref}
  end
end
