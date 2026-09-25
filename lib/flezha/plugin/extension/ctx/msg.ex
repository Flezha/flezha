defmodule Flezha.Plugin.Extension.Ctx.Msg do
  @moduledoc """
  Context struct for the `:on_message` extension (kind: `:request`). The
  `key` field selects which subscriber handles the request — a string, as the
  hook's `key` sub-option is, so a user-chosen key is never
  interned. `reply_to` and `ref` form the reply envelope.
  """

  @type t() :: %__MODULE__{
          key: binary(),
          payload: binary(),
          reply_to: pid(),
          ref: reference(),
          operator_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil
        }

  defstruct [
    :key,
    :payload,
    :reply_to,
    :ref,
    :operator_id,
    :namespace,
    :runner_id
  ]
end
