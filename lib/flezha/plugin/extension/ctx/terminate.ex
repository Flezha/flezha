defmodule Flezha.Plugin.Extension.Ctx.Terminate do
  @moduledoc """
  Context struct for the `:on_terminate` extension (kind: `:fanout`).
  Subscribers may observe the termination reason.
  """

  @type t() :: %__MODULE__{
          operative_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          reason: term()
        }

  defstruct [:operative_id, :namespace, :runner_id, :reason]
end
