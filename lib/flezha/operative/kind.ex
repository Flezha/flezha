defmodule Flezha.Operative.Kind do
  @moduledoc """
  Operative kinds and the capabilities each kind declares.

  A kind is set at start and frozen for the operative's lifetime. Each declares
  a SPARSE capability map listing only what it has; `capable?/2` reads every
  capability with a `false` default, so a capability a kind does not declare is
  absent. `:stream` declares `persistent_state?`, `time_travel?`,
  `dynamic_entities?` and `output_stall_watch?`; `:workflow` is a run-once job
  and declares none.

  Invariant: gating is subtractive only. Adding a flag means adding the one
  control-plane gate that reads it through `capable?/2` and opting the kinds
  that need it into `@capabilities` — a kind that does not opt in reads it
  `false` and is unaffected.
  """

  @type kind() :: atom()
  @type capability() :: atom()

  @capabilities %{
    stream: %{
      persistent_state?: true,
      time_travel?: true,
      dynamic_entities?: true,
      output_stall_watch?: true
    },
    workflow: %{}
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
