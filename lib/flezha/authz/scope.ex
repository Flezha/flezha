defmodule Flezha.AuthZ.Scope do
  @moduledoc """
  An authorization scope is a map carrying at minimum a `:namespace`
  binding and optionally a resource specifier (e.g., `:operator`,
  `:plugin_id`).

  Service callers construct one inline at every
  `Flezha.AuthZ.authorize/3` callsite.
  """

  @type t :: %{
          required(:namespace) => binary(),
          optional(atom()) => term()
        }

  @doc """
  Validate the shape of a scope map.
  """
  @spec validate(term()) :: :ok | {:error, :invalid_scope}
  def validate(%{namespace: ns}) when is_binary(ns), do: :ok
  def validate(_), do: {:error, :invalid_scope}

  @doc "Construct a scope from a namespace and optional keyword extras."
  @spec build(binary(), keyword()) :: t()
  def build(namespace, extras \\ []) when is_binary(namespace) do
    Enum.into(extras, %{namespace: namespace})
  end
end
