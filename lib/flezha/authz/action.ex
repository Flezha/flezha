defmodule Flezha.AuthZ.Action do
  @moduledoc """
  Compile-time enumeration of every authorizable action atom.

  Add an entry here AND a role-grant in `Flezha.AuthZ.Policy` before
  calling `Flezha.AuthZ.authorize/3` with a new action.
  """

  @actions [
    # Operative lifecycle and read.
    :"operative.start",
    :"operative.stop",
    :"operative.list",
    # Plugin lifecycle and read.
    :"plugin.install",
    :"plugin.enable",
    :"plugin.disable",
    :"plugin.uninstall",
    :"plugin.upgrade",
    :"plugin.list",
    :"plugin.info",
    # Audit log read / mutate.
    :"audit.read",
    :"audit.rotate",
    # Observability read / mutate.
    :"observability.query",
    :"observability.subscribe",
    :"observability.enable_source",
    :"observability.disable_source",
    # Operational reset.
    :"reset.run",
    # Auth-state mutations.
    :"authz.bootstrap",
    :"authz.token_issue",
    :"authz.token_revoke",
    :"authz.token_list",
    :"authz.whoami",
    :"authz.namespace_create",
    :"authz.namespace_delete",
    :"authz.namespace_list",
    :"authz.namespace_show",
    :"authz.namespace_set_quota",
    :"authz.role_bind",
    :"authz.role_unbind",
    :"authz.role_list",
    # Data-plane verbs granted only to the executor role.
    :"executor.connect",
    :"executor.checkpoint",
    :"executor.report",
    # Controller / boot / state surface.
    :"controller.status",
    :"controller.stop",
    :"boot.status",
    :"boot.await",
    :"boot.import_legacy_snapshot",
    :"state.list",
    :"state.at",
    :"state.rotate",
    :"state.retention"
  ]

  @doc """
  Returns the full set of known action atoms.
  """
  @spec all() :: [atom()]
  def all, do: @actions

  @doc """
  True when `action` is a recognised member of the enumeration.
  """
  @spec known?(atom()) :: boolean()
  def known?(action) when is_atom(action), do: action in @actions
end
