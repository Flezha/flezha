defmodule Flezha.Process.DurableTasksTest do
  @moduledoc """
  Durable work runs under the controller's task supervisor, and still runs,
  without taking its caller down, when that supervisor is gone or stopping.

  Each test acts on the application's own supervisor, so none runs alongside
  another.
  """
  use ExUnit.Case, async: false

  alias Flezha.Process.DurableTasks

  defp restart_on_exit(),
    do: on_exit(fn -> Supervisor.restart_child(Flezha.Supervisor, DurableTasks) end)

  test "the work runs under the supervisor" do
    test = self()

    assert {:ok, task} =
             DurableTasks.start(fn ->
               receive do
                 :go -> send(test, :ran)
               end
             end)

    assert task in Task.Supervisor.children(DurableTasks)
    send(task, :go)
    assert_receive :ran
  end

  test "with the supervisor stopped, the work runs and the caller does not exit" do
    :ok = Supervisor.terminate_child(Flezha.Supervisor, DurableTasks)
    restart_on_exit()
    test = self()

    assert {:ok, _pid} = DurableTasks.start(fn -> send(test, :ran) end)
    assert_receive :ran
  end

  test "with the supervisor stopping, its name still registered, the work runs and the caller does not exit" do
    test = self()

    # Holds the supervisor in its shutdown until released.
    {:ok, holder} =
      Task.Supervisor.start_child(
        DurableTasks,
        fn ->
          Process.flag(:trap_exit, true)
          send(test, :holding)

          receive do
            {:EXIT, _sup, :shutdown} -> send(test, :stopping)
          end

          receive do
            :release -> :ok
          end
        end,
        shutdown: :infinity
      )

    assert_receive :holding
    spawn(fn -> Supervisor.terminate_child(Flezha.Supervisor, DurableTasks) end)
    restart_on_exit()
    # Registered last so it runs first: a failed assertion must not leave the stop held.
    on_exit(fn -> send(holder, :release) end)
    assert_receive :stopping

    sup = Process.whereis(DurableTasks)
    assert is_pid(sup)
    :erlang.trace(sup, true, [:receive, {:tracer, test}])

    caller = Task.async(fn -> DurableTasks.start(fn -> send(test, :ran) end) end)

    assert_receive {:trace, ^sup, :receive,
                    {:"$gen_call", _from, {:start_task, _args, _restart, _shutdown}}}

    send(holder, :release)

    assert {:ok, _pid} = Task.await(caller)
    assert_receive :ran
  end
end
