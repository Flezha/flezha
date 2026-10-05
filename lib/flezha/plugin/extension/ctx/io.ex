defmodule Flezha.Plugin.Extension.Ctx.IO do
  @moduledoc """
  Context struct for the `:on_input` / `:on_output` extensions (kind:
  `:transform`). Carries the in-flight payload along with relay identity.
  """

  @type t() :: %__MODULE__{
          relay_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          message: binary(),
          metadata: map()
        }

  defstruct [:relay_id, :namespace, :runner_id, :message, metadata: %{}]
end
