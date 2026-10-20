defmodule Flezha.AuthZ.SourceRegistration do
  @moduledoc """
  The `:platform_authz` source, so authz signals (`:authorization_denied`,
  `:token_issued`, `:token_revoked`, `:token_expiring`, `:bootstrap_completed`,
  `:rebootstrap_recovery`) participate in fan-out and the audit log allow-list.
  The observability registry registers it; the authz modules emit through its
  `emit/3`.
  """

  use Flezha.Observability.Source,
    name: :platform_authz,
    cardinality_budget: 256,
    area: :controller,
    title: "Access control",
    description:
      "Calls refused for lack of a role, tokens issued and revoked, and the first administrator's setup.",
    signals: [
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
    ]
end
