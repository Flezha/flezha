defmodule FlezhaTest.Helper.Telemetry do
  @moduledoc """
  One telemetry capture for the whole suite.

  Attaching a handler that forwards emissions to the test process and detaches
  at exit was written four times over, once per subsystem that needed it, each
  copy differing only in the shape of the message it sent. This is that
  mechanism once.

      Telemetry.capture([:flezha, :plugin, :kafka, :shipped])
      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], meas, meta}

  `capture/2` takes one event or a list of them. `:as` shapes the message the
  test receives — it is given the event, the measurements and the metadata —
  and `:filter` is asked about the metadata before anything is sent, so a test
  waiting on one plugin's events is not woken by another's.
  """

  @type event :: [atom()]

  @doc "Forward every emission of `events` to the calling process."
  @spec capture(event() | [event()], keyword()) :: :ok
  def capture(events, opts \\ []) do
    events = normalise(events)
    test_pid = self()
    handler_id = {__MODULE__, events, System.unique_integer([:positive])}
    shape = Keyword.get(opts, :as, &default_shape/3)
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

  defp default_shape(event, measurements, metadata),
    do: {:telemetry, event, measurements, metadata}

  # One event is a list of atoms; many is a list of those.
  defp normalise([head | _] = events) when is_list(head), do: events
  defp normalise(event) when is_list(event), do: [event]
end
