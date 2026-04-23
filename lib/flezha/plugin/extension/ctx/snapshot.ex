defmodule Flezha.Plugin.Extension.Ctx.Snapshot do
  @moduledoc """
  Context struct for the `:on_snapshot` extension (kind: `:fanout`). Carries
  the produced snapshot bytes (or nil on failure) and processor identity.
  """

  @type t() :: %__MODULE__{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          compute_engine_id: String.t() | nil,
          snapshot: binary() | nil
        }

  defstruct [:processor_id, :namespace, :compute_engine_id, :snapshot]
end
