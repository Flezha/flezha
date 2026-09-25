defmodule Flezha.Plugin.Extension.Ctx.Terminate do
  @moduledoc """
  Context struct for the `:on_terminate` extension (kind: `:fanout`).
  Subscribers may observe the termination reason.
  """

  @type t() :: %__MODULE__{
          operator_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          reason: term()
        }

  defstruct [:operator_id, :namespace, :runner_id, :reason]
end
