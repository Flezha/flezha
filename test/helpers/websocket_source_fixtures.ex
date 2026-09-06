defmodule FlezhaTest.Helper.WebsocketSourceFixtures do
  @moduledoc """
  The subscription-source fixture the `Plugin.Provider.Websocket` reconcile
  tests are written against: a client state, a `Subscription`, one settled poll
  driven through `handle_info/2`, and the readers that take a request id, a
  method or an item list back out of a frame.

  Driving the loop at its own seam rather than through a live socket is what
  lets a test assert on the exact frames a given source transition produces.
  """


  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.Websocket.Subscription

  @sub_template ~s({"method":"subscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})
  @unsub_template ~s({"method":"unsubscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})

  @doc "The subscribe template every fixture subscription renders."
  def sub_template, do: @sub_template

  @doc "The unsubscribe template every fixture subscription renders."
  def unsub_template, do: @unsub_template

  def subscription(overrides \\ %{}) do
    struct!(
      Subscription,
      Map.merge(
        %{
          url: "https://cfg.internal/subs",
          headers: %{},
          # Long enough that the rescheduled poll never fires mid-test.
          interval_ms: 600_000,
          subscribe_template: @sub_template,
          unsubscribe_template: @unsub_template,
          ack_mode: "required"
        },
        overrides
      )
    )
  end

  def state(overrides \\ %{}) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        # `self()` so the `{:io_in, …}` ingest and the applied-set persist cast
        # are both assertable.
        overwatcher_pid: self(),
        payload_metadata: %{},
        idle_timeout_ms: 10_000,
        resubscribe_after_ms: 0,
        ignore_pattern: nil,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now,
        subscription: subscription(),
        applied: MapSet.new(),
        desired: nil,
        inflight: %{},
        next_req_id: 1,
        cycle: 0,
        poll: nil,
        timers: Flezha.Process.Timer.new(),
        source_etag: nil,
        last_reconcile: nil,
        last_error: nil,
        source_plugin_id: "ws_plugin",
        source_namespace: "default",
        source_operative_id: "op-1"
      },
      Map.new(overrides)
    )
  end

  @doc "Drive one settled poll through the reconcile loop."
  def poll(state, result) do
    pid = spawn(fn -> :ok end)

    Websocket.Client.handle_info({:subscription_fetched, pid, result}, %{
      state
      | poll: {pid, make_ref()}
    })
  end

  def source(items, version \\ 1) do
    {:ok, Jason.encode!(%{"version" => version, "items" => items}), nil}
  end

  def req_id(frame) do
    {:text, json} = frame
    Jason.decode!(json)["req_id"]
  end

  def items_of(frame) do
    {:text, json} = frame
    Jason.decode!(json)["params"]["symbol"]
  end

  def method_of(frame) do
    {:text, json} = frame
    Jason.decode!(json)["method"]
  end

  def ack(req_id, extra \\ %{}) do
    {:text, Jason.encode!(Map.merge(%{"req_id" => req_id, "success" => true}, extra))}
  end

  def attach_telemetry(event), do: FlezhaTest.Helper.Telemetry.capture(event)
end
