defmodule Flezha.Operative.Kind do
  @moduledoc """
  Source of truth for operative *kinds* and the *capabilities* each kind declares.

  Every operative carries a `kind` (an atom, set at start and frozen for its
  lifetime). Each kind declares a **sparse** capability descriptor: a map
  listing only the capabilities the kind *has* (as `true`). The control plane
  reads every capability with a `false` default (`capable?/2`), so a capability
  a kind does not declare is treated as absent.

  A `kind` is what flavour of compute unit a operative is. Capabilities attach to the
  kind, never to user-authored compute-unit code — choosing a kind is sufficient
  and an SDK author never enumerates flags.

  ## Capabilities

    * `:persistent_state?` — durable per-Entity write-ahead log, snapshots,
      and restore, plus per-Entity state introspection. The gate for
      crash-resume and entity-state queries.
    * `:time_travel?` — point-in-time state queries and retention beyond the
      latest snapshot (gates `flz state at`).
    * `:dynamic_entities?` — lazy per-key Entity creation and destructive
      idle eviction with cold-start-fresh.
    * `:output_stall_watch?` — the per-Entity output-staleness watchdog that
      forces a rebind.

  `:unbounded` declares all four. `:bounded` is a run-once, stateless job and
  declares none, so every flag reads `false`.

  ## Extension protocol

  Adding a capability flag later (e.g. `:restart_on_boot?`, `:scheduled?`) is
  additive:

    1. Add the one control-plane gate that reads the flag via `capable?/2`
       (undeclared flags read `false`).
    2. Opt the built-in kinds that need it into it here, in `@capabilities`.

  A kind that does not opt in reads the new flag as `false` and is unaffected.
  When a new flag names a behavior the full `:unbounded` kind needs, opt `:unbounded`
  into it in the same change that adds the gate, so the full kind never silently
  loses a behavior.
  """

  @type kind() :: atom()
  @type capability() :: atom()

  @capabilities %{
    unbounded: %{
      persistent_state?: true,
      time_travel?: true,
      dynamic_entities?: true,
      output_stall_watch?: true
    },
    bounded: %{}
  }

  @doc "Sparse capability descriptor for `kind` (declared-true capabilities only)."
  @spec capabilities(kind()) :: %{optional(capability()) => boolean()}
  def capabilities(kind), do: Map.get(@capabilities, kind, %{})

  @doc "Read one capability for `kind`, defaulting an undeclared flag to `false`."
  @spec capable?(kind(), capability()) :: boolean()
  def capable?(kind, capability), do: Map.get(capabilities(kind), capability, false)

  @doc "Whether `kind` is a recognized operative kind."
  @spec known?(kind()) :: boolean()
  def known?(kind), do: Map.has_key?(@capabilities, kind)
end
