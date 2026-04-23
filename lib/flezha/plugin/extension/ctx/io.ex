defmodule Flezha.Plugin.Extension.Ctx.IO do
  @moduledoc """
  Context struct for the `:on_input` / `:on_output` extensions (kind:
  `:transform`). Carries the in-flight payload along with processor identity.
  """

  @type t() :: %__MODULE__{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t() | nil,
          message: binary(),
          metadata: map()
        }

  defstruct [:processor_id, :namespace, :compute_engine_id, :message, metadata: %{}]
end
