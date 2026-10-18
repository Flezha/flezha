defmodule FlezhaTest.Helper.MessageTestHelper do
  @moduledoc """
  The calling process's mailbox as a test reads it: a condition re-checked as
  each awaited message lands, the messages a pattern matches collected in
  order, and a mailbox emptied.
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
