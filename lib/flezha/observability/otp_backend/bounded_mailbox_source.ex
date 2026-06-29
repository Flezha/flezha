defmodule Flezha.Observability.OtpBackend.BoundedMailboxSource do
  @moduledoc """
  Polled source for the bounded-mailbox subsystem.

  Cardinality budget rationale: one metadata shape per
  `{namespace, operative}`. A cluster running `R` operatives across `N`
  namespaces emits up to `R × N` distinct shapes per metric name.
  A `5_000` budget covers ~50 namespaces × ~100 operatives comfortably;
  larger deployments raise it via runtime config.

  Bounded-mailbox state currently lives on the Rust SDK side and is
  not surfaced on the Elixir Controller. The source registers so it
  is visible in `flz observe sources`, and emits a one-shot
  `:bounded_mailbox_unavailable` event on first poll noting that
  metrics will start flowing once the NIF surface exposes them.
  This mirrors the foundation's degraded-source pattern.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :flezha_bounded_mailbox`.
  """

  use Flezha.Observability.OtpBackend.Source,
    source: :flezha_bounded_mailbox,
    default_poll_ms: 1_000

  alias Flezha.Observability.{Bus, Registry, Signal}

  @impl GenServer
  def init(_opts) do
    Registry.register_source(%{
      name: @source,
      kinds: [:metric, :event],
      flavour: :process_backed,
      cardinality_budget: 5_000
    })

    interval = poll_interval_ms()
    Process.send_after(self(), :poll, interval)
    {:ok, %{interval: interval, announced?: false}}
  end

  @impl GenServer
  def handle_info(:poll, %{announced?: false} = state) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :bounded_mailbox_unavailable,
        measurements: %{count: 1},
        metadata: %{reason: :nif_surface_pending}
      })
    )

    Process.send_after(self(), :poll, state.interval)
    {:noreply, %{state | announced?: true}}
  end

  def handle_info(:poll, state) do
    poll_mailboxes()
    Process.send_after(self(), :poll, state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Internals ----------

  defp poll_mailboxes() do
    # Future hook: enumerate per-Operative state from the NIF and emit
    # :mailbox_high_water (gauge) / :messages_dropped (counter) signals.
    :ok
  end
end
