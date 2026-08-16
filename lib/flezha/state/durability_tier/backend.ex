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
  store a fencing arbiter: a manifest write carries the writer's WAL epoch and
  is refused when a strictly higher epoch already landed. A refusal means
  another claimant owns this entity's remote lineage — the loser stops shipping
  rather than retrying (`docs/remote_durability_tier.md`).

  `resolve/0` is the one place configuration becomes a `{module, opts}` pair;
  `:none` (the default) means the tier is off and nothing is started.
  """

  @type key :: binary()
  @type blob :: binary()
  @type opts :: keyword()
  @type epoch :: non_neg_integer()

  @doc "Store `blob` at `key`, overwriting unconditionally."
  @callback put(key(), blob(), opts()) :: :ok | {:error, term()}

  @doc "Read the blob at `key`."
  @callback get(key(), opts()) :: {:ok, blob()} | {:error, :not_found | term()}

  @doc "Every key under `prefix`."
  @callback list(binary(), opts()) :: {:ok, [key()]} | {:error, term()}

  @doc """
  Store `blob` at `key` stamped with `epoch`, refusing when the stored epoch is
  strictly higher. Equal epochs overwrite — a shipping controller updates its
  own manifest many times within one epoch.
  """
  @callback put_if(key(), blob(), epoch(), opts()) ::
              :ok | {:error, {:stale_epoch, epoch()} | term()}

  @doc "Remove `key`. Missing keys succeed — GC is idempotent by construction."
  @callback delete(key(), opts()) :: :ok | {:error, term()}

  @doc """
  Child spec for whatever connection the backend needs, or `:ignore` for a
  connectionless one. Started by the tier supervisor ahead of everything that
  ships or hydrates.
  """
  @callback child_spec(opts()) :: Supervisor.child_spec() | :ignore

  @doc """
  The configured backend as `{module, opts}`, or `:none` when the tier is off.

  Reads `:flezha, :durability_tier`. The shipped names are `:postgres` and
  `:s3`; an explicit `{module, opts}` tuple selects any module implementing
  this behaviour, which is the extension point that keeps "adding a backend is
  a module, not a fork" true. Anything else is `:none` — a typo must leave the
  controller running on local durability, never crash it into a boot loop.
  """
  @spec resolve() :: {module(), opts()} | :none
  def resolve() do
    config = Application.get_env(:flezha, :durability_tier, [])

    case Keyword.get(config, :backend, :none) do
      :postgres -> {__MODULE__.Postgres, Keyword.get(config, :backend_opts, [])}
      :s3 -> {__MODULE__.S3, Keyword.get(config, :backend_opts, [])}
      {module, opts} when is_atom(module) and is_list(opts) -> {module, opts}
      _ -> :none
    end
  end
end
