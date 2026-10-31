defmodule Flezha.State.CreditLedger do
  @moduledoc """
  Pure byte-credit ledger for credit-grant flow control: the receiver
  debits work on receipt, durable drain frees the bytes, and freed
  credit is granted back in batches — never per record.

  Accounting only — no processes, no side effects — so one core serves
  every credit loop: the runner→controller state stream
  (`Flezha.Runner.StreamChannel` drives it and performs the sends) and
  the per-Relay source-ingestion ledger in `Flezha.State.InputLog`.
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

  @doc """
  Bytes debited but not yet durably drained — the work the receiver is still
  holding. Every admission and grant decision is a comparison against this.
  """
  def outstanding(%__MODULE__{} = ledger), do: ledger.debited - ledger.drained

  @doc "Remaining credit (window minus un-drained debits). Negative on overshoot."
  def level(%__MODULE__{} = ledger), do: ledger.window - outstanding(ledger)

  @doc """
  Admit `size` bytes of work: `{:ok, ledger}` debits them;
  `{:overrun, ledger}` refuses a frame past the exhausted window — except
  that work arriving on an empty buffer is always admitted, even when
  larger than the whole window (single-frame overshoot; the sender side
  mirrors this by sending at a full window).
  """
  def admit(%__MODULE__{} = ledger, size) do
    un_drained = outstanding(ledger)

    if un_drained > 0 and un_drained + size > ledger.window do
      {:overrun, %{ledger | overrun?: true}}
    else
      {:ok, %{ledger | debited: ledger.debited + size, overrun?: false}}
    end
  end

  @doc "Take back `size` debited bytes whose work was dropped before it ever drained."
  def refund(%__MODULE__{} = ledger, size), do: %{ledger | debited: ledger.debited - size}

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

    # Either trigger earns a grant, and the sender needs both: a drained buffer
    # keeps a trickling sender alive, and `low` bytes freed keeps a sender that
    # actually fills its window moving. Without the second, a saturating sender
    # sees nothing until the buffer halves, so it parks, bursts, parks again.
    # Granting earlier is safe by construction — `granted` never exceeds
    # `drained`, so a grant only returns credit already made durable.
    if freed > 0 and (outstanding(ledger) < ledger.low or freed >= ledger.low) do
      {freed, %{ledger | granted: ledger.drained}}
    else
      {0, ledger}
    end
  end
end
