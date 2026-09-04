defmodule Flezha.Service.CallerContext do
  @moduledoc """
  The context every service-API call carries as its last argument, and its
  validation.

  The shape is locked here so a caller — the CLI today, a UI later — is
  validated the same way by every service module. What a verb is authorized
  as, where, and how it is audited is declared once in `Flezha.AuthZ.Verbs`
  and applied by `Flezha.AuthZ.Verb`.
  """

  @valid_sources [:cli, :ui, :sdk, :internal]

  @type t :: Flezha.AuthZ.caller_context()

  @spec validate(t()) :: :ok | {:error, :invalid_caller_context}
  def validate(%{source: src}) when src in @valid_sources, do: :ok
  def validate(_), do: {:error, :invalid_caller_context}
end
