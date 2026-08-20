defmodule Flezha.State.CreditLedger do
  @moduledoc """
  Pure byte-credit ledger for credit-grant flow control: the receiver
  debits work on receipt, durable drain frees the bytes, and freed
  credit is granted back in low-watermark batches — never per record.

  Accounting only — no processes, no side effects — so one core serves
  every credit loop: today the runner→controller state stream
  (`Flezha.Runner.StreamChannel` drives it and performs the sends),
  and the per-Operative source-ingestion ledger when the
  effectively-once tier's input-log lands.
  """

  @enforce_keys [:session, :window, :low]
  defstruct [:session, :window, :low, debited: 0, drained: 0, granted: 0, overrun?: false]

  @type t :: %__MODULE__{
          session: binary(),
          window: pos_integer(),
          low: pos_integer(),
          debited: non_neg_integer(),
          drained: non_neg_integer(),
          granted: non_neg_integer(),
          overrun?: boolean()
        }

  def new(session, window, low) when window > 0 and low > 0 do
    %__MODULE__{session: session, window: window, low: low}
  end

  @doc "Remaining credit (window minus un-drained debits). Negative on overshoot."
  def level(%__MODULE__{} = ledger) do
    ledger.window - (ledger.debited - ledger.drained)
  end

  @doc """
  Admit `size` bytes of work: `{:ok, ledger}` debits them;
  `{:overrun, ledger}` refuses a frame past the exhausted window — except
  that work arriving on an empty buffer is always admitted, even when
  larger than the whole window (single-frame overshoot; the sender side
  mirrors this by sending at a full window).
  """
  def admit(%__MODULE__{} = ledger, size) do
    un_drained = ledger.debited - ledger.drained

    if un_drained > 0 and un_drained + size > ledger.window do
      {:overrun, %{ledger | overrun?: true}}
    else
      {:ok, %{ledger | debited: ledger.debited + size, overrun?: false}}
    end
  end

  @doc """
  Register `bytes` of durable drain. Returns `{grant, ledger}`: `grant` is
  the batched replenishment owed to the sender — all freed-but-ungranted
  bytes once either the un-drained buffer falls below the low watermark or
  `low` bytes have been freed since the last grant, `0` otherwise (a stalled
  drain mints nothing).
  """
  def drain(%__MODULE__{} = ledger, bytes) do
    ledger = %{ledger | drained: ledger.drained + bytes}
    freed = ledger.drained - ledger.granted

    # Two ways to earn a grant, and the sender needs both:
    #
    #   * the un-drained buffer fell below the low watermark — a trickling
    #     sender stays alive even though little has been freed;
    #   * at least `low` bytes have been freed since the last grant — a sender
    #     that actually fills its window keeps moving.
    #
    # The second is what makes a full window usable. On the first condition
    # alone, a sender saturating the window sees no grant until the buffer
    # drains below half, so it parks, bursts, and parks again — throughput
    # collapses to that sawtooth exactly when the pipeline is busiest.
    # Granting earlier is always safe: `granted` never exceeds `drained`, so a
    # grant only returns credit the writer has already made durable.
    if freed > 0 and (ledger.debited - ledger.drained < ledger.low or freed >= ledger.low) do
      {freed, %{ledger | granted: ledger.drained}}
    else
      {0, ledger}
    end
  end
end
