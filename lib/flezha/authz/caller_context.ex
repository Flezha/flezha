defmodule Flezha.Service.CallerContext do
  @moduledoc """
  The context every service-API call carries as its last argument, and its
  validation.

  The shape is locked here so a caller — the CLI today, a UI later — is
  validated the same way by every service module. What a verb is authorized
  as, where, and how it is audited is declared once in `Flezha.AuthZ.Verbs`
  and applied by `Flezha.AuthZ.Verb`.
  """

  alias Flezha.AuthZ.TokenStore

  @valid_sources [:cli, :ui, :sdk, :http, :internal]

  # The sources that reach the controller over the network: a runner
  # connection and HTTP. The others run on the controller's own distribution.
  @networked_sources [:sdk, :http]

  @type t :: Flezha.AuthZ.caller_context()

  @doc """
  The context of a caller from `source` whose identity is the verified `token`:
  its principal, namespace and role, and nothing the caller wrote itself.
  """
  @spec from_token(atom(), binary()) :: {:ok, t()} | {:error, Flezha.Error.t()}
  def from_token(source, token) do
    with {:ok, %{principal: principal, namespace: namespace, role: role}} <-
           TokenStore.verify(token),
         do: {:ok, %{source: source, principal: principal, namespace: namespace, role: role}}
  end

  @doc "Whether the caller reaches the controller over the network rather than its own node."
  @spec networked?(t()) :: boolean()
  def networked?(caller_context), do: Map.get(caller_context, :source) in @networked_sources

  @spec validate(t()) :: :ok | {:error, :invalid_caller_context}
  def validate(%{source: src}) when src in @valid_sources, do: :ok
  def validate(_), do: {:error, :invalid_caller_context}
end
