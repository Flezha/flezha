defmodule FlezhaTest.Helper.MessageTestHelper do
  @moduledoc """
  The calling process's mailbox as a test reads it: a telemetry event turned
  into an ordinary message, the messages a pattern matches collected in order,
  and a mailbox emptied.
  """

  @doc """
  Stand in for the convergence loop: register the calling process under the
  loop's name so every `Flezha.Convergence.Facts.declare/1` a site makes lands
  here as `{:declare, kind, target, args}`. The loop is off in the test
  config, so the name is free; it dies with the test process.

      become_loop()
      assert_receive {:declare, :seq_gap, ^cell_id, %{runner_id: "e1"}}
  """
  def become_loop() do
    Process.register(self(), Flezha.Convergence.Loop)
    :ok
  end

  @doc """
  Forward every occurrence of `event` to the calling process as
  `{tag, measurements, metadata}` for the duration of the test, so a telemetry
  signal can be awaited with `assert_receive` like any other message. The
  handler is detached on exit.

      forward_telemetry([:flezha, :transport, :connect_rejected], :rejected)
      assert_receive {:rejected, %{count: 1}, %{stage: :handshake}}, 500

  `:telemetry` handlers are VM-global: the handler fires for an event executed
  in ANY process, not only this test's. In an `async: true` module that makes an
  unfiltered forward unsafe — a concurrently running emitter's event lands in
  this mailbox, and `assert_receive` then matches a signal the test never
  caused while `refute_received` is falsified outright. Pass `filter`, a
  predicate on the event's metadata, to scope the observation to what this test
  itself emitted:

      forward_telemetry(event, :fail_open, &(Map.get(&1, :plugin_id) == my_id))

  The predicate runs in the emitting process, so keep it total — a raise there
  makes `:telemetry` detach the handler and the test goes deaf.
  """
  def forward_telemetry(event, tag, filter \\ fn _metadata -> true end)
      when is_function(filter, 1) do
    FlezhaTest.Helper.Telemetry.capture(event,
      filter: filter,
      as: fn _event, measurements, metadata -> {tag, measurements, metadata} end
    )
  end

  def recv_num(msg, count) do
    for _i <- 1..count do
      receive do
        ^msg -> :ok
      after
        2000 -> raise "Timeout waiting for message"
      end
    end
  end

  def recv_drain(msg, timeout \\ 2000) do
    receive do
      ^msg -> 1 + recv_drain(msg, timeout)
    after
      timeout -> 0
    end
  end

  def flush(msg) do
    receive do
      ^msg -> flush(msg)
    after
      0 -> :ok
    end
  end

  def recv_until(msg, condition, timeout \\ 5000) do
    if condition.() do
      :ok
    else
      receive do
        ^msg -> recv_until(msg, condition, timeout)
      after
        timeout -> raise "Condition not met within #{timeout}ms"
      end
    end
  end

  @doc """
  Every message one of the clauses matches, in arrival order, each as its
  clause's body returns it. Stops once no matching message arrives within the
  `after:` milliseconds (0 by default: only what is already queued). A message
  no clause matches stays in the mailbox.

      import FlezhaTest.Helper.MessageTestHelper, only: [collect: 1, collect: 2]

      collect do
        {:counted, signal} -> signal
      end

      collect after: 100 do
        {:flz_data, _, _, message, _} -> message
      end
  """
  defmacro collect(opts \\ [], do: clauses) do
    clauses =
      Enum.map(clauses, fn {:->, meta, [head, body]} -> {:->, meta, [head, {:ok, body}]} end)

    quote do
      FlezhaTest.Helper.MessageTestHelper.collect_next(
        fn timeout ->
          receive do
            unquote(clauses)
          after
            timeout -> :none
          end
        end,
        Keyword.get(unquote(opts), :after, 0)
      )
    end
  end

  @doc false
  def collect_next(next, timeout) do
    case next.(timeout) do
      {:ok, value} -> [value | collect_next(next, timeout)]
      :none -> []
    end
  end

  @doc "Empty the calling process's mailbox."
  def flush_all() do
    receive do
      _ -> flush_all()
    after
      0 -> :ok
    end
  end
end
