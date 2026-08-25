defmodule Flezha.Plugin.Extension.Ctx.Msg do
  @moduledoc """
  Context struct for the `:on_message` extension (kind: `:request`). The
  `key` field selects which subscriber handles the request — a string, as the
  subscription's `key` sub-option is, so an operator-chosen key is never
  interned. `reply_to` and `ref` form the reply envelope.
  """

  @type t() :: %__MODULE__{
          key: binary(),
          payload: binary(),
          reply_to: pid(),
          ref: reference(),
          operative_id: String.t(),
          namespace: atom() | String.t(),
          runner_id: String.t() | nil
        }

  defstruct [
    :key,
    :payload,
    :reply_to,
    :ref,
    :operative_id,
    :namespace,
    :runner_id
  ]
end
