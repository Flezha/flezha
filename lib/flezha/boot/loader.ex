defmodule Flezha.Boot.Loader do
  @moduledoc """
  The controller's boot sequence, in one process: pre-hooks, the prerequisites
  each enabled transport needs (the UDS socket directory, the QUIC certificates),
  post-hooks, then a bounded wait for every namespace hub to leave ProcessHub's
  recovery state machine. Answers `:boot_ready?` once that is done.

  Recovery is waited for, never depended on: a hub that does not reach `:normal`
  within `:boot_replay_timeout_ms` is logged and boot continues, because the
  marker-gated recovery has its own continuation and the convergence loop
  corrects the drift.
  """

  require Logger

  use GenServer

  alias Flezha.Config.Durations
  alias Flezha.Service.HubManager
  alias Flezha.Socket.Support
  alias Flezha.Transport.CertGenerator

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    run_pre_hooks()

    transports = Application.get_env(:flezha, :transports, [:uds])

    if :uds in transports do
      Support.prepare_socket_dir()
    end

    if :quic in transports do
      CertGenerator.ensure_certs()
    end

    {:ok, %{boot_ready: false}, {:continue, :boot}}
  end

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()
    await_namespace_hubs()
    log_attach_line()

    {:noreply, Map.put(state, :boot_ready, true)}
  end

  defp log_attach_line() do
    case attach_line() do
      nil -> :ok
      line -> Logger.info(line)
    end
  end

  @doc """
  How a runner reaches this controller: the absolute socket directory and the
  assignment that attaches to it, plus the QUIC listen address when QUIC is
  enabled. Logged on reaching ready so an operator never has to find the
  documentation. `nil` when this node listens on no runner transport.
  """
  @spec attach_line() :: String.t() | nil
  def attach_line() do
    transports = Application.get_env(:flezha, :transports, [:uds])

    parts =
      Enum.concat([
        if(:uds in transports,
          do: [
            "runners attach with FLEZHA_SOCKET_FILES_PATH=#{Support.socket_dir()}"
          ],
          else: []
        ),
        if(:quic in transports,
          do: ["QUIC listening on #{Application.get_env(:flezha, :quic_listen_address)}"],
          else: []
        )
      ])

    if parts != [], do: "[flezha] " <> Enum.join(parts, "; ")
  end

  # Wait for every namespace hub to leave the ProcessHub recovery state
  # machine (`:recovery_pending` → `:recovering` | `:normal`). Bounded by
  # `:flezha, :boot_replay_timeout_ms`. Failures are logged but do not
  # block boot — the marker-gated recovery has its own background
  # continuation and the convergence watcher (phase 4) corrects drift.
  defp await_namespace_hubs() do
    timeout_ms = Durations.get(:boot_replay_timeout_ms)

    try do
      case HubManager.await_recovery_complete(timeout_ms) do
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
  def handle_info({:runner_connected_event, _}, state) do
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
