defmodule Flezha.AuthZ.SourceRegistration do
  @moduledoc """
  One-shot worker that registers the `:platform_authz` source on the
  observability bus so authz signals (`:authorization_denied`,
  `:token_issued`, `:token_revoked`, `:token_expiring`,
  `:namespace_created`, `:role_bound`, `:bootstrap_completed`, ...)
  participate in fan-out and the audit log allow-list.
  """

  use GenServer, restart: :transient

  alias Flezha.Observability.Registry

  @source :platform_authz

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case safe_register() do
      :ok -> :ignore
      {:error, _} -> :ignore
    end
  end

  defp safe_register() do
    Registry.register_source(%{
      name: @source,
      kinds: [:event, :metric],
      flavour: :declarative,
      cardinality_budget: 256
    })
  rescue
    _ -> {:error, :registry_unavailable}
  catch
    :exit, _ -> {:error, :registry_unavailable}
  end
end
