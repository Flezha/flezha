defmodule Flezha.AuthZ.Action do
  @moduledoc """
  Compile-time enumeration of every authorizable action atom.

  An action exists only because a row of `Flezha.AuthZ.Verbs` names it: add
  the entry here, a role grant in `Flezha.AuthZ.Policy` and the row together,
  and the declaration test refuses an action no row names.
  """

  @actions [
    # Relay lifecycle and read.
    :"relay.start",
    :"relay.stop",
    :"relay.list",
    :"relay.scale",
    :"relay.rewind",
    :"relay.send",
    :"relay.replay",
    :"relay.listen",
    # Plugin lifecycle and read.
    :"plugin.enable",
    :"plugin.disable",
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
    # Auth-state mutations. The bootstrap claim is not here: the one-time
    # token IS its credential, so nothing can authorize it.
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
    # The one data-plane verb the runner role holds.
    :"runner.connect",
    # Dropping a runner process from the fleet, and lifting the drop.
    :"runner.drop",
    # Controller / state surface.
    :"controller.status",
    :"controller.stop",
    :"state.get",
    :"state.list",
    :"state.at",
    :"state.rotate",
    :"state.retention",
    :"state.delete",
    # Records in flight: an instance's inbox, journal and outbox.
    :"state.records",
    # Dead-letter-queue administration verbs. The reads are named separately from the
    # mutations because `dlq.show` is the one that answers payload bytes.
    :"dlq.list",
    :"dlq.show",
    :"dlq.replay",
    :"dlq.drop",
    # Scheduled relay spawns.
    :"schedule.list",
    :"schedule.add",
    :"schedule.remove"
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
