defmodule Flezha.Plugin.Extension.Ctx.Init do
  @moduledoc """
  Context struct for the `:on_operative_init` extension (kind: `:fold`).
  `accumulated` is threaded through each subscriber.
  """

  @type t() :: %__MODULE__{
          operative_id: String.t(),
          namespace: atom() | String.t(),
          factory_id: String.t(),
          accumulated: map()
        }

  defstruct [:operative_id, :namespace, :factory_id, accumulated: %{}]
end
