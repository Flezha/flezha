defmodule Flezha.Plugin.Extension.Ctx.Init do
  @moduledoc """
  Context struct for the `:on_processor_init` extension (kind: `:fold`).
  `accumulated` is threaded through each subscriber.
  """

  @type t() :: %__MODULE__{
          processor_id: String.t(),
          namespace: atom() | String.t(),
          factory_id: String.t(),
          accumulated: map()
        }

  defstruct [:processor_id, :namespace, :factory_id, accumulated: %{}]
end
