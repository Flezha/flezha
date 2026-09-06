defmodule Flezha.Boot.Loader do
  @moduledoc """
  The controller's boot sequence, in one process: pre-hooks, the prerequisites
  each enabled transport needs (the UDS socket directory, the QUIC certificates),
  post-hooks, then the attach line.

  Readiness is not answered here. `Flezha.Boot.Readiness` folds it from a
  precondition table evaluated on every read, so a probe is never queued behind
  this process's mailbox and a hub that recovers late needs no restart. What
  the loader owns is the *sequence*, and `await_sequence/0` is the one call
  that says it has run.
  """

  require Logger

  use GenServer

  alias Flezha.Config
  alias Flezha.Socket.Support
  alias Flezha.Transport.CertGenerator

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    run_pre_hooks()

    transports = Config.get(:transports)

    if :uds in transports do
      Support.prepare_socket_dir()
    end

    if :quic in transports do
      CertGenerator.ensure_certs()
    end

    {:ok, %{}, {:continue, :boot}}
  end

  @doc """
  Block until the boot continuation has run. The call queues behind that
  continuation in this process's mailbox, and that ordering is the whole
  guarantee: it is the only fact that says the sequence finished, and the
  30 s allowance is what turns a wedged hook into a named exit rather than a
  hang.
  """
  @spec await_sequence() :: :ok
  def await_sequence(), do: GenServer.call(__MODULE__, :await_sequence, 30_000)

  @impl true
  def handle_continue(:boot, state) do
    run_post_hooks()
    log_attach_line()

    {:noreply, state}
  end

  @impl true
  def handle_call(:await_sequence, _from, state) do
    {:reply, :ok, state}
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
  enabled. Logged once the sequence completes so an operator never has to
  find the documentation. `nil` when this node listens on no runner transport.
  """
  @spec attach_line() :: String.t() | nil
  def attach_line() do
    transports = Config.get(:transports)

    parts =
      Enum.concat([
        if(:uds in transports,
          do: [
            "runners attach with #{Config.env_name(:socket_files_path)}=#{Support.socket_dir()}"
          ],
          else: []
        ),
        if(:quic in transports,
          do: ["QUIC listening on #{Config.get(:quic_listen_address)}"],
          else: []
        )
      ])

    if parts != [], do: "[flezha] " <> Enum.join(parts, "; ")
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
    Config.get(:pre_hooks)
    |> Enum.each(& &1.())
  end

  defp run_post_hooks() do
    Config.get(:post_hooks)
    |> Enum.each(& &1.())
  end
end
