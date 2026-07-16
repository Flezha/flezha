defmodule Flezha.Plugin.Extension.Ctx.IO do
  @moduledoc """
  Context struct for the `:on_input` / `:on_output` extensions (kind:
  `:transform`). Carries the in-flight payload along with operative identity.
  """

  @type t() :: %__MODULE__{
          operative_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          message: binary(),
          metadata: map()
        }

  defstruct [:operative_id, :namespace, :runner_id, :message, metadata: %{}]
end
