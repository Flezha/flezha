defmodule Flezha.Operative.Builder do
  @moduledoc """
  Constructor for operative specs handed to
  `Flezha.Service.OperativeManager.start_operative/2`.

  The `opts` keyword list is opaque to most callers; four keys are
  contractual — `:default_input_type` (the tag the Overwatch reads when
  metadata omits one), `:plugins` (subscribed at operative start),
  `:seed_input` (delivered as the instance's first input during its bind) and
  `:operative_class` (the on-disk persistence key, defaulting to `operative.id`).

  Invariant: `:kind` is set at start and frozen for the operative's lifetime.
  See `Flezha.Operative.Kind` for the kind/capability model.
  """

  @type t() :: %__MODULE__{
          id: String.t(),
          factory_id: String.t(),
          kind: atom(),
          init_values: map(),
          opts: Keyword.t()
        }
  @enforce_keys [:id, :factory_id]
  defstruct [:id, :factory_id, kind: :stream, init_values: %{}, opts: []]
end
