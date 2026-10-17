defmodule Flezha.AuthZ.SourceRegistration do
  @moduledoc """
  One-shot worker that registers the `:platform_authz` source on the
  observability bus so authz signals (`:authorization_denied`,
  `:token_issued`, `:token_revoked`, `:token_expiring`,
  `:namespace_created`, `:role_bound`, `:bootstrap_completed`, ...)
  participate in fan-out and the audit log allow-list.
  """

  use GenServer, restart: :transient

  alias Flezha.Observability.{Registry, Source}

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

  @doc "The `:platform_authz` source's registration."
  @spec registration() :: map()
  def registration() do
    %{
      name: @source,
      kinds: [:event, :metric],
      flavour: :declarative,
      cardinality_budget: 256,
      area: :controller,
      description: "Authorization: refused calls, tokens and the first-time bootstrap.",
      signals:
        Source.signals(
          authorization_denied: {:event, "A call was refused for lack of a role."},
          token_issued: {:event, "A token was issued."},
          token_expiring: {:event, "A token is close to expiring."},
          token_revoked: {:event, "A token was revoked."},
          bootstrap_completed: {:event, "The first administrator was set up."},
          rebootstrap_recovery: {:event, "Authorization was set up again to recover access."}
        )
    }
  end

  defp safe_register() do
    Registry.register_source(registration())
  rescue
    _ -> {:error, :registry_unavailable}
  catch
    :exit, _ -> {:error, :registry_unavailable}
  end
end
