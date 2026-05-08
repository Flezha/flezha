defmodule Flezha.Plugin.Extension.Ctx.Terminate do
  @moduledoc """
  Context struct for the `:on_terminate` extension (kind: `:fanout`).
  Subscribers may observe the termination reason.
  """

  @type t() :: %__MODULE__{
          relay_id: String.t(),
          namespace: atom() | String.t(),
          executor_id: String.t() | nil,
          reason: term()
        }

  defstruct [:relay_id, :namespace, :executor_id, :reason]
end
