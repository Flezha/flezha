defmodule Flezha.Observability.OtpBackend.BoundedMailboxSource do
  @moduledoc """
  Polled source for the bounded-mailbox subsystem.

  Bounded-mailbox state lives on the SDK side and is not surfaced on
  the Controller yet, so this source registers — to be visible in
  `flz observe sources` — emits one `:bounded_mailbox_unavailable`
  event on its first poll, and does nothing else. The foundation's
  degraded-source pattern.

  The `5_000` budget is one metadata shape per `{namespace, operator}`:
  ~50 namespaces × ~100 operators, raised through runtime config for
  anything larger.
  """

  use Flezha.Observability.OtpBackend.Poller,
    source: :flezha_bounded_mailbox,
    kinds: [:metric, :event],
    cardinality_budget: 5_000,
    default_poll_ms: 1_000

  alias Flezha.Observability.{Bus, Signal}

  @impl Flezha.Observability.OtpBackend.Poller
  def init_state(_opts), do: %{announced?: false}

  @impl Flezha.Observability.OtpBackend.Poller
  def poll(%{announced?: false} = state) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :bounded_mailbox_unavailable,
        measurements: %{count: 1},
        metadata: %{reason: :nif_surface_pending}
      })
    )

    %{state | announced?: true}
  end

  def poll(state) do
    # Future hook: enumerate per-Operator state from the NIF and emit
    # :mailbox_high_water (gauge) / :messages_dropped (counter) signals.
    state
  end
end
