defmodule Flezha.Plugin.Extension.Ctx.Terminate do
  @moduledoc """
  Context struct for the `:on_terminate` extension (kind: `:fanout`).
  Subscribers may observe the termination reason.
  """

  @type t() :: %__MODULE__{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t() | nil,
          reason: term()
        }

  defstruct [:processor_id, :namespace, :compute_engine_id, :reason]
end
