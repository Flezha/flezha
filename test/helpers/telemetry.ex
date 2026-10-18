defmodule FlezhaTest.Helper.Telemetry do
  @moduledoc """
  One telemetry capture for the whole suite.

  Attaching a handler that forwards emissions to the test process and detaches
  at exit was written four times over, once per subsystem that needed it, each
  copy differing only in the shape of the message it sent. This is that
  mechanism once.

      Telemetry.capture([:flezha, :plugin, :http_push, :shipped])
      assert_receive {:telemetry, [:flezha, :plugin, :http_push, :shipped], meas, meta}

  `capture/2` takes one event or a list of them. `:as` shapes the message the
  test receives: a function is given the event, the measurements and the
  metadata, and any other term is a tag, sent as `{tag, measurements, metadata}`.

      Telemetry.capture([:flezha, :stream, :drop], as: :drop)
      assert_receive {:drop, %{count: 1}, _meta}

  A handler fires for an event executed in any process, so a test that shares
  the VM with others passes `:filter`, a predicate on the metadata asked before
  anything is sent. It runs in the emitting process: a raise there detaches the
  handler and the test goes deaf.
  """

  @type event :: [atom()]

  @doc "Forward every emission of `events` to the calling process."
  @spec capture(event() | [event()], keyword()) :: :ok
  def capture(events, opts \\ []) do
    events = normalise(events)
    test_pid = self()
    handler_id = {__MODULE__, events, System.unique_integer([:positive])}
    shape = opts |> Keyword.get(:as, &default_shape/3) |> shape()
    filter = Keyword.get(opts, :filter, fn _meta -> true end)

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, _config ->
          if filter.(metadata), do: send(test_pid, shape.(event, measurements, metadata))
        end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp shape(shape) when is_function(shape, 3), do: shape
  defp shape(tag), do: fn _event, measurements, metadata -> {tag, measurements, metadata} end

  defp default_shape(event, measurements, metadata),
    do: {:telemetry, event, measurements, metadata}

  # One event is a list of atoms; many is a list of those.
  defp normalise([head | _] = events) when is_list(head), do: events
  defp normalise(event) when is_list(event), do: [event]
end
