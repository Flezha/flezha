defmodule Flezha.Plugin.Extension.Ctx.Snapshot do
  @moduledoc """
  Context struct for the `:on_snapshot` extension (kind: `:fanout`). Carries
  the produced snapshot bytes (or nil on failure) and operative identity.
  """

  @type t() :: %__MODULE__{
          operative_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil,
          snapshot: binary() | nil
        }

  defstruct [:operative_id, :namespace, :runner_id, :snapshot]
end
