defmodule Flezha.Shutdown.SignalHandler do
  @moduledoc """
  Owns the controller's OS signal dispatch so SIGTERM enters graceful
  shutdown in every BEAM run mode (release, mix, iex, escript). Started
  **first** in the application supervision tree so the handler is in
  place before any other service can observe a delivered signal.

  Signals reach this GenServer through `erl_signal_server`, the kernel
  `gen_event` that ERTS notifies. `Relay` is registered there and
  **replaces** kernel's own `erl_signal_handler`: that handler calls
  `init:stop()` on `sigterm` by itself, which would drive the shutdown
  through `Flezha.Application.prep_stop/1` and record it as
  `reason: :prep_stop` roughly a millisecond before this server could
  dispatch. The two cannot both own SIGTERM.

  Taking that ownership means also keeping the semantics kernel's
  handler provided for the other signals it owned — `sigquit` and
  `sigusr1` still halt — so only SIGTERM's disposition changes.

  Signal escalation:
    1. First SIGTERM — log the arrival and invoke
       `Flezha.Shutdown.Coordinator.shutdown(reason)` then
       `:init.stop()`. The `[:flezha, :shutdown, :requested]` event is
       the Coordinator's to emit, from the reason passed here. The trailing stop re-enters `prep_stop/1`, whose
       `Coordinator.shutdown(:prep_stop)` lands on the coordinator's
       `phase: :done` short-circuit, so it costs a reply and not a
       second drain.
    2. Second signal during shutdown — log + ignore.
    3. Third signal — `:erlang.halt(143)` escape hatch.

  SIGINT is **not** claimed. ERTS reserves it for the break handler and
  `:os.set_signal/2` does not accept it — `sigint` is absent from the
  BIF's signal list and the call raises `badarg` in every run mode. A
  release runs with the break handler off (`+Bd` in `rel/vm.args.eex`),
  so SIGINT ends that node at once, with no drain.

  When `:flezha, :graceful_shutdown_enabled` is `false`, this server
  still starts but installs nothing and registers nothing, leaving
  kernel's handler in place.

  If the install or the registration fails, the failure is caught and
  `:degraded` is latched. Every signal this server installs is one ERTS
  permits, so the latch always means the platform refused rather than that
  an impossible call was attempted. `Coordinator.status/0`
  reads that state from here on demand; it is not pushed, because this
  server starts before the Coordinator exists.
  """

  use GenServer, shutdown: 5_000
  require Logger

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.SafeCall
  alias Flezha.Shutdown.Coordinator

  defstruct count: 0, enabled: true, status: :ok, signal_server: :erl_signal_server

  defmodule Relay do
    @moduledoc false
    # `erl_signal_server` gen_event callback. Forwards and nothing else:
    # gen_event removes a handler that raises, and with kernel's handler
    # replaced that would leave the node unable to react to SIGTERM.
    @behaviour :gen_event

    @doc """
    Take ownership of `manager`'s signals for `owner`. Ours goes in BEFORE
    kernel's comes out, so a failure leaves kernel's handler owning the
    signal instead of nothing owning it. `terminate/2` is the inverse.
    """
    def install(manager, owner) do
      :ok = :gen_event.add_sup_handler(manager, __MODULE__, {owner, manager})
      _ = :gen_event.delete_handler(manager, :erl_signal_handler, [])
      :ok
    end

    @impl true
    def init({owner, manager}), do: {:ok, {owner, manager}}

    @impl true
    def handle_event(signal, {owner, _manager} = state) do
      send(owner, signal)
      {:ok, state}
    end

    @impl true
    def handle_call(_request, state), do: {:ok, :ok, state}

    @impl true
    def handle_info(_msg, state), do: {:ok, state}

    # The inverse of `install/2`: degrade back to kernel's `init:stop()`
    # rather than to a node that ignores SIGTERM entirely. This runs INSIDE
    # the manager, so the restore is handed to another process —
    # `add_handler` and `which_handlers` are synchronous calls that would
    # deadlock against the manager that is executing this callback.
    @impl true
    def terminate(_reason, {_owner, manager}) do
      if manager == :erl_signal_server do
        spawn(fn ->
          unless :erl_signal_handler in :gen_event.which_handlers(manager) do
            _ = :erl_signal_handler.start()
          end
        end)
      end

      :ok
    end
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  The handler's own install state. `Flezha.Shutdown.Coordinator.status/0`
  reads this rather than caching a pushed value — this server is started
  before the Coordinator, so a push at install time has nowhere to land.
  """
  @spec status() :: :ok | :degraded
  def status() do
    SafeCall.bounded(__MODULE__, :status, Durations.get(:signal_handler_status_call_ms),
      on_error: :degraded
    )
  end

  @impl true
  def init(opts) do
    enabled =
      Keyword.get_lazy(opts, :enabled, fn ->
        Config.get(:graceful_shutdown_enabled)
      end)

    state = %__MODULE__{
      enabled: enabled,
      signal_server: Keyword.get(opts, :signal_server, :erl_signal_server)
    }

    {:ok, state, {:continue, :install_signal_handler}}
  end

  @impl true
  def handle_continue(:install_signal_handler, %__MODULE__{enabled: false} = state) do
    Logger.info("[shutdown] graceful_shutdown_enabled=false; OS signal handlers NOT installed")

    {:noreply, state}
  end

  def handle_continue(:install_signal_handler, %__MODULE__{enabled: true} = state) do
    state =
      try do
        # SIGTERM is already `handle` by default; assert it explicitly so the
        # disposition is not left to the platform.
        :os.set_signal(:sigterm, :handle)
        :ok = Relay.install(state.signal_server, self())

        Logger.info("[shutdown] OS signal handlers installed (SIGTERM)")
        %{state | status: :ok}
      catch
        kind, reason ->
          Logger.warning(
            "[shutdown] os.set_signal unavailable: #{inspect({kind, reason})}; " <>
              "falling back to release-mode SIGTERM"
          )

          %{state | status: :degraded}
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(signal, state) when signal in [:sigterm, :sigint],
    do: handle_signal(signal, state)

  # Kernel's `erl_signal_handler` owned these before Relay replaced it;
  # its behaviour is reproduced so only SIGTERM's disposition changes.
  def handle_info(:sigquit, state) do
    :erlang.halt()
    {:noreply, state}
  end

  def handle_info(:sigusr1, state) do
    :erlang.halt(~c"Received SIGUSR1")
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, state.status, state}
  end

  defp handle_signal(signal, %__MODULE__{count: 0} = state) do
    Logger.info("[shutdown] received #{signal}")

    Task.start(fn ->
      try do
        Coordinator.shutdown(signal)
      catch
        :exit, reason ->
          Logger.warning("[shutdown] coordinator exit during signal handling: #{inspect(reason)}")
      end

      :init.stop()
    end)

    {:noreply, %{state | count: 1}}
  end

  defp handle_signal(signal, %__MODULE__{count: 1} = state) do
    Logger.info("[shutdown] already in progress, ignoring #{signal}")
    {:noreply, %{state | count: 2}}
  end

  defp handle_signal(signal, %__MODULE__{count: c} = state) when c >= 2 do
    Logger.warning("[shutdown] forced halt requested via third #{signal}")
    # `halt/1` does not flush the Logger, and this WARN is the only record
    # an operator gets of a forced halt.
    Logger.flush()
    :erlang.halt(143)
    {:noreply, state}
  end
end
