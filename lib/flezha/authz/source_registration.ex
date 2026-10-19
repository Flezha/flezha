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
      title: "Access control",
      description:
        "Calls refused for lack of a role, tokens issued and revoked, and the first administrator's setup.",
      signals:
        Source.signals(
          authorization_denied:
            {:event,
             label: "Access denied",
             outcome: :failure,
             meaning: "A call was refused because the caller lacks the role it needs."},
          token_issued: {:event, label: "Token issued", meaning: "A token was issued."},
          token_expiring: {:event, label: "Token expiring", meaning: "A token will expire soon."},
          token_revoked: {:event, label: "Token revoked", meaning: "A token was revoked."},
          bootstrap_completed:
            {:event, label: "First admin set up", meaning: "The first administrator was set up."},
          rebootstrap_recovery:
            {:event,
             label: "Access recovered",
             meaning: "Access control was set up again to recover lost access."}
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
