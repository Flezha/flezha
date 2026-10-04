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

  @valid_sources [:cli, :ui, :sdk, :http, :mcp, :internal]

  # The sources held to what the network may reach: a runner connection, HTTP,
  # and an AI assistant through `flz mcp`, which rides the controller's own
  # distribution but must reach no more than one over HTTP. The others run on
  # that distribution with the operator's full reach.
  @networked_sources [:sdk, :http, :mcp]

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

  @doc "Whether the caller is held to what the network may reach: remote operations that do not stream."
  @spec networked?(t()) :: boolean()
  def networked?(caller_context), do: Map.get(caller_context, :source) in @networked_sources

  @spec validate(t()) :: :ok | {:error, :invalid_caller_context}
  def validate(%{source: src}) when src in @valid_sources, do: :ok
  def validate(_), do: {:error, :invalid_caller_context}
end
