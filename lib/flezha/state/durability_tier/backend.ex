defmodule Flezha.State.DurabilityTier.Backend do
  @moduledoc """
  The remote durability tier's storage contract: dumb blob operations, nothing
  else.

  A backend stores opaque byte blobs under opaque keys. It never learns WAL
  framing, seq ranges, torn-tail repair, truncation or supersession — all of
  that is `Flezha.State.DurabilityTier` and is shared across every backend, so
  adding one is a day's work rather than a re-implementation of crash
  consistency.

  `put_if/4` is the one conditional operation, and it is what makes the remote
  store a fencing arbiter: a manifest write carries a claim counter and is
  refused when another writer holds the lineage. The counter is stored by the
  backend and read by nobody else, which is the whole point — it is the one fact
  about a reference that never travels with the local state, so it separates two
  writers where the WAL epoch, copied by migration, cannot.
  """

  @type key :: binary()
  @type blob :: binary()
  @type opts :: keyword()
  @type epoch :: non_neg_integer()
  @type age_ms :: non_neg_integer()
  @type claim :: non_neg_integer()
  @type guard :: {:extend, claim()} | {:claim, claim()}

  @doc "Store `blob` at `key`, overwriting unconditionally."
  @callback put(key(), blob(), opts()) :: :ok | {:error, term()}

  @doc "Read the blob at `key`."
  @callback get(key(), opts()) :: {:ok, blob()} | {:error, :not_found | term()}

  @doc """
  Every key under `prefix`, each with how long ago it was last written.

  The age rather than the timestamp: a backend that keeps its own clock
  (Postgres) computes it server-side, so collection's grace never depends on
  agreement between the controller's clock and the store's.
  """
  @callback list(binary(), opts()) :: {:ok, [{key(), age_ms()}]} | {:error, term()}

  @doc """
  Store `blob` at `key` under a conditional guard, and report the stored counter
  when refused.

    * `{:extend, n}` — apply unless the stored counter is strictly higher. This
      is a writer continuing its own lineage, which it does many times over.
    * `{:claim, n}` — apply only if the stored counter is strictly lower. This
      is a writer taking a lineage over, and the strict compare is what makes it
      a compare-and-set: two nodes bidding the same `n` cannot both win.

  Both modes are needed. `<=` alone would let simultaneous claimants overwrite
  each other; `<` alone would refuse the owner's ordinary repeat writes.
  """
  @callback put_if(key(), blob(), guard(), opts()) ::
              :ok | {:error, {:claim_held, claim()} | term()}

  @doc "Remove `key`. Missing keys succeed — GC is idempotent by construction."
  @callback delete(key(), opts()) :: :ok | {:error, term()}

  @doc """
  Child spec for whatever connection the backend needs, or `:ignore` for a
  connectionless one. Started by the tier supervisor ahead of everything that
  ships or hydrates.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec() | :ignore

  @doc """
  Option keys this backend cannot work without, checked once at boot.

  A backend that reaches `Keyword.fetch!/2` for an option only at the first
  ship looks configured for as long as nothing ships: the posture reports a
  remote tier and the store is empty. Declaring the keys here is what turns
  that into a refusal naming the option, at the boot that selected the backend.
  """
  @callback required_opts() :: [atom()]

  @optional_callbacks required_opts: 0

  @doc """
  Whether `guard` admits a write against the counter the store already holds.

  One definition, so two backends cannot drift from the contract they both
  implement — a divergence here is a silent correctness bug, not a failing call.
  A backend that can express it in its own query language (Postgres) does so
  instead; this is for the ones that compare in Elixir.
  """
  @spec guard_passes?(guard(), claim()) :: boolean()
  def guard_passes?({:extend, claim}, stored), do: stored <= claim
  def guard_passes?({:claim, claim}, stored), do: stored < claim
end
