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

  # Wait for every namespace hub to leave the ProcessHub recovery state
  # machine (`:recovery_pending` → `:recovering` | `:normal`). Bounded by
  # `:flezha, :boot_replay_timeout_ms`. Failures are logged but do not
  # block boot — the marker-gated recovery has its own background
  # continuation and the convergence watcher (phase 4) corrects drift.
  defp await_namespace_hubs() do
    timeout_ms = Application.get_env(:flezha, :boot_replay_timeout_ms, 60_000)

    try do
      case Flezha.Service.HubManager.await_recovery_complete(timeout_ms) do
        :ok ->
          :ok

        {:error, {:recovery_timeout, hubs}} ->
          Logger.warning("[boot] recovery timeout for hubs=#{inspect(hubs)}")
      end
    catch
      kind, reason ->
        Logger.warning("[boot] await_recovery_complete raised #{kind}: #{inspect(reason)}")
    end
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
