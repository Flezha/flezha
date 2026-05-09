defmodule Flezha.BootLoader do
  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    run_pre_hooks()

    transports = Application.get_env(:flezha, :transports, [:uds])

    if :uds in transports do
      Flezha.Socket.Support.prepare_socket_dir()
    end

    if :quic in transports do
      Flezha.Transport.CertGenerator.ensure_certs()
    end

    {:ok, %{boot_ready: false}, {:continue, :boot}}
  end

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()
    await_namespace_hubs()

    {:noreply, Map.put(state, :boot_ready, true)}
  end

  # After post-hooks fire, wait for every namespace hub to leave the
  # ProcessHub recovery state machine (`:recovery_pending` →
  # `:recovering` | `:normal`). Bounded by `:flezha,
  # :boot_replay_timeout_ms` per hub. Failures are logged but do not
  # block boot — auto_recovery has its own background continuation.
  defp await_namespace_hubs() do
    timeout_ms = Application.get_env(:flezha, :boot_replay_timeout_ms, 60_000)

    Flezha.Service.HubManager.get_running_namespaces()
    |> Enum.each(fn namespace ->
      hub_id = Flezha.Service.Identificator.hub(namespace)

      try do
        case ProcessHub.await_normal(hub_id, timeout_ms) do
          :ok -> :ok
          other -> Logger.warning("[boot] await_normal #{hub_id} -> #{inspect(other)}")
        end
      catch
        kind, reason ->
          Logger.warning("[boot] await_normal #{hub_id} raised #{kind}: #{inspect(reason)}")
      end
    end)
  end

  @impl true
  def handle_call(:boot_ready?, _from, state) do
    {:reply, Map.get(state, :boot_ready, false), state}
  end

  @impl true
  def handle_info({:executor_connected_event, _}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.error("Unexpected message at boot validator: #{inspect(msg)} on node #{node()}")

    {:noreply, state}
  end

  defp run_pre_hooks() do
    Application.get_env(:flezha, :pre_hooks, [])
    |> Enum.each(& &1.())
  end

  defp run_post_hooks() do
    Application.get_env(:flezha, :post_hooks, [])
    |> Enum.each(& &1.())
  end
end
