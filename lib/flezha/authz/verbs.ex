defmodule Flezha.AuthZ.Verbs do
  @moduledoc """
  The declaration of every operator verb: one row per verb, naming the action
  the policy judges, the rule that resolves the namespace it is judged in, and
  the audit it leaves. `Flezha.AuthZ.Verb.run/5` is the one function that
  applies a row, on every surface.

  Rows are data and name no module. What needs one — the function a wire name
  runs, the fetch a `{:row, _}` scope reads, the device a refusal prints on —
  is supplied by the surface that binds the row.

  A row's fields:

    * `action` — an atom of `Flezha.AuthZ.Action`, or `:public` for a read
      served to any caller, or `:none` for a wire name that is not a verb.
    * `scope` — where the action is judged: `{:argument, n}` (the nth positional
      argument is the namespace; `""` means `default`, `nil` means `_system`),
      `:system`, `:caller` (the context's own namespace, `_system` when it has
      none) or `{:row, field}` (a field of a row the binding fetches, read only
      after the caller is bound and its role grants the action).
    * `audit` — `:none`, or `{:before | :after, source}`; an `:after` audit
      carries the outcome.
    * `audit_name` — the record's name when it is not the action's verb.
    * `target` — labels for the leading positional arguments the audit records;
      `:_` skips one.
  """

  alias Flezha.AuthZ.Action

  @public [action: :public]
  @none [action: :none]

  @rows [
    # Liveness surfaces and the helpers a consumer names: no policy question.
    boot_status: @public,
    boot_await: @public,
    kind_read: @public,
    partition_report: @public,
    cron_arithmetic: @public,
    retention_default: @public,
    authz_bootstrap: @none,
    build_mem_top_snapshot: @none,
    group_partition_rows: @none,
    # Audit log: every read is itself audited, under the function's own name.
    audit_list: [
      action: :"audit.read",
      scope: :caller,
      audit: {:before, :audit_log},
      audit_name: :list_files
    ],
    audit_read: [
      action: :"audit.read",
      scope: :caller,
      audit: {:before, :audit_log},
      audit_name: :read,
      target: [:file]
    ],
    audit_tail: [
      action: :"audit.read",
      scope: :caller,
      audit: {:before, :audit_log},
      audit_name: :tail
    ],
    audit_stats: [
      action: :"audit.read",
      scope: :caller,
      audit: {:before, :audit_log},
      audit_name: :stats
    ],
    audit_rotate: [
      action: :"audit.rotate",
      scope: :caller,
      audit: {:before, :audit_log},
      audit_name: :rotate
    ],
    audit_unsubscribe: [action: :"audit.read", scope: :caller],
    # Identity, tokens, roles and namespaces. The token store records its own
    # issue and revoke; the role and namespace stores record nothing, so those
    # verbs are audited here.
    authz_whoami: [action: :"authz.whoami", scope: :caller],
    token_issue: [action: :"authz.token_issue", scope: {:argument, 2}],
    token_revoke: [action: :"authz.token_revoke", scope: {:row, :namespace}],
    token_list: [action: :"authz.token_list", scope: :caller],
    role_bind: [
      action: :"authz.role_bind",
      scope: {:argument, 3},
      audit: {:after, :namespace_service},
      target: [:subject, :role]
    ],
    role_unbind: [
      action: :"authz.role_unbind",
      scope: {:row, :namespace},
      audit: {:after, :namespace_service},
      # An empty role in the record means the principal's whole binding.
      target: [:subject, :role]
    ],
    role_list: [action: :"authz.role_list", scope: :caller],
    namespace_create: [
      action: :"authz.namespace_create",
      scope: {:argument, 1},
      audit: {:after, :namespace_service}
    ],
    namespace_delete: [
      action: :"authz.namespace_delete",
      scope: {:argument, 1},
      audit: {:after, :namespace_service}
    ],
    namespace_set_quota: [
      action: :"authz.namespace_set_quota",
      scope: {:argument, 1},
      audit: {:after, :namespace_service},
      target: [:_, :quotas]
    ],
    namespace_show: [action: :"authz.namespace_show", scope: {:argument, 1}],
    namespace_list: [action: :"authz.namespace_list", scope: :system],
    namespace_roles: [action: :"authz.role_list", scope: {:argument, 1}],
    # The controller's own state.
    controller_status: [action: :"controller.status", scope: :caller],
    controller_mem_top: [action: :"controller.status", scope: :caller],
    controller_hub_id: [action: :"controller.status", scope: {:argument, 1}],
    controller_recovery: [action: :"operative.list", scope: {:argument, 1}],
    controller_stop: [
      action: :"controller.stop",
      scope: :system,
      audit: {:after, :controller_service}
    ],
    cluster_status: [action: :"operative.list", scope: {:argument, 1}],
    # Listings.
    list_namespaces: [action: :"operative.list", scope: :caller],
    list_operatives: [action: :"operative.list", scope: {:argument, 1}],
    list_runners: [action: :"operative.list", scope: {:argument, 1}],
    operative_info: [action: :"operative.list", scope: {:argument, 1}],
    operative_classes: [action: :"operative.list", scope: :caller],
    # Operative lifecycle and live work.
    operative_start: [
      action: :"operative.start",
      scope: {:argument, 1},
      audit: {:after, :operative_service},
      target: [:_, :operative]
    ],
    operative_stop: [
      action: :"operative.stop",
      scope: {:argument, 1},
      audit: {:after, :operative_service},
      target: [:_, :operative]
    ],
    operative_scale: [
      action: :"operative.scale",
      scope: {:argument, 1},
      audit: {:after, :workflow_service},
      target: [:_, :group, :partitions]
    ],
    operative_rewind: [
      action: :"operative.rewind",
      scope: {:argument, 1},
      audit: {:after, :workflow_service},
      target: [:_, :operative, :to_iteration]
    ],
    operative_send: [
      action: :"operative.send",
      scope: {:argument, 1},
      audit: {:after, :operative_service},
      target: [:_, :operative]
    ],
    # A tasklet's `ctx.spawn_workflow` feeding a running instance: the right
    # `operative_send` judges, with no record of its own because it runs once per
    # event. A refusal is still audited, by the policy point.
    spawn_deliver: [action: :"operative.send", scope: {:argument, 1}],
    operative_replay: [
      action: :"operative.replay",
      scope: {:row, :namespace},
      audit: {:after, :operative_service},
      target: [:_, :cell]
    ],
    operative_listen: [
      action: :"operative.listen",
      scope: {:argument, 1},
      audit: {:after, :plugin_service},
      target: [:_, :operative]
    ],
    cell_state: [action: :"state.get", scope: {:argument, 1}],
    schedule_run: [
      action: :"operative.start",
      scope: {:row, :namespace},
      audit: {:after, :operative_service},
      target: [:schedule]
    ],
    runner_connect: [action: :"runner.connect", scope: {:argument, 1}],
    # Plugins: the installed catalog is controller-wide, an enabled plugin lives
    # in its operative's namespace.
    plugin_list: [action: :"plugin.list", scope: :caller],
    list_installed_plugins: [action: :"plugin.list", scope: :caller],
    plugin_info: [action: :"plugin.info", scope: :caller],
    plugin_enable: [
      action: :"plugin.enable",
      scope: {:row, :namespace},
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    plugin_disable: [
      action: :"plugin.disable",
      scope: {:row, :namespace},
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    install_plugin: [
      action: :"plugin.install",
      scope: :system,
      audit: {:after, :plugin_service},
      target: [:path]
    ],
    upgrade_plugin: [
      action: :"plugin.upgrade",
      scope: :system,
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    uninstall_plugin: [
      action: :"plugin.uninstall",
      scope: :system,
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    installer_enable_plugin: [
      action: :"plugin.enable",
      scope: :system,
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    installer_disable_plugin: [
      action: :"plugin.disable",
      scope: :system,
      audit: {:after, :plugin_service},
      target: [:plugin_id]
    ],
    # Observability: the mutations and the subscriptions are audited before the
    # effect, as the subsystem has always done.
    observability_sources: [action: :"observability.query", scope: :caller],
    observability_sinks: [action: :"observability.query", scope: :caller],
    observability_query: [action: :"observability.query", scope: :caller],
    observability_subscribe: [
      action: :"observability.subscribe",
      scope: :caller,
      audit: {:before, :observability_service},
      target: [:source]
    ],
    observability_unsubscribe: [
      action: :"observability.subscribe",
      scope: :caller,
      audit: {:before, :observability_service},
      audit_name: :unsubscribe,
      target: [:source]
    ],
    observability_tail: [
      action: :"observability.subscribe",
      scope: :caller,
      audit: {:before, :observability_service},
      audit_name: :tail,
      target: [:source]
    ],
    observability_enable: [
      action: :"observability.enable_source",
      scope: :caller,
      audit: {:before, :observability_service},
      target: [:source]
    ],
    observability_disable: [
      action: :"observability.disable_source",
      scope: :caller,
      audit: {:before, :observability_service},
      target: [:source]
    ],
    # Dead-letter queue: judged in the named class's namespace, `_system` when
    # none is named.
    dlq_list: [action: :"dlq.list", scope: {:row, :namespace}],
    dlq_stats: [action: :"dlq.list", scope: :system],
    dlq_show: [action: :"dlq.show", scope: {:row, :namespace}],
    dlq_drop: [
      action: :"dlq.drop",
      scope: {:row, :namespace},
      audit: {:after, :dlq_service},
      target: [:entry]
    ],
    dlq_replay: [
      action: :"dlq.replay",
      scope: {:row, :namespace},
      audit: {:after, :dlq_service},
      target: [:entry]
    ],
    # Schedules: one registry for the whole controller.
    schedule_list: [action: :"schedule.list", scope: :system],
    schedule_add: [
      action: :"schedule.add",
      scope: :system,
      audit: {:after, :schedule_service},
      target: [:schedule]
    ],
    schedule_remove: [
      action: :"schedule.remove",
      scope: :system,
      audit: {:after, :schedule_service},
      target: [:schedule]
    ],
    schedule_assert: [
      action: :"schedule.assert",
      scope: :system,
      audit: {:after, :schedule_service},
      target: [:schedule]
    ],
    schedule_retract: [
      action: :"schedule.retract",
      scope: :system,
      audit: {:after, :schedule_service},
      target: [:schedule]
    ],
    # Durable state: a Cell's namespace is its class's.
    state_get: [action: :"state.get", scope: {:row, :namespace}],
    state_list: [action: :"state.list", scope: {:row, :namespace}],
    state_at: [action: :"state.at", scope: {:row, :namespace}],
    state_rotate: [
      action: :"state.rotate",
      scope: {:row, :namespace},
      audit: {:after, :state_service},
      target: [:cell]
    ],
    state_delete: [
      action: :"state.delete",
      scope: {:row, :namespace},
      audit: {:after, :state_service},
      target: [:cell]
    ],
    state_retention: [
      action: :"state.retention",
      scope: :system,
      audit: {:after, :state_service},
      target: [:operative_class, :hours]
    ],
    state_outbox: [action: :"state.records", scope: {:row, :namespace}],
    state_inbox: [action: :"state.records", scope: {:row, :namespace}],
    state_journal: [action: :"state.records", scope: {:row, :namespace}],
    state_metrics: [action: :"state.list", scope: :system]
  ]

  @type scope :: {:argument, pos_integer()} | :system | :caller | {:row, atom()}
  @type audit :: :none | {:before | :after, atom()}
  @type t :: %{
          verb: atom(),
          action: atom(),
          scope: scope() | nil,
          audit: audit(),
          audit_name: atom() | nil,
          target: [atom()]
        }

  @keys [:action, :scope, :audit, :audit_name, :target]
  @unscoped [:public, :none]

  for {verb, fields} <- @rows do
    unknown = Keyword.keys(fields) -- @keys
    action = Keyword.fetch!(fields, :action)
    scope = Keyword.get(fields, :scope)
    audit = Keyword.get(fields, :audit, :none)

    cond do
      unknown != [] ->
        raise ArgumentError, "verb #{inspect(verb)} has unknown fields #{inspect(unknown)}"

      action not in @unscoped and not Action.known?(action) ->
        raise ArgumentError,
              "verb #{inspect(verb)} names #{inspect(action)}, which Flezha.AuthZ.Action does not"

      action in @unscoped and (scope != nil or audit != :none) ->
        raise ArgumentError, "verb #{inspect(verb)} is #{action} and carries a scope or an audit"

      action not in @unscoped and
          not (match?({:argument, n} when is_integer(n) and n > 0, scope) or
                 scope in [:system, :caller] or match?({:row, f} when is_atom(f), scope)) ->
        raise ArgumentError, "verb #{inspect(verb)} has no scope rule (got #{inspect(scope)})"

      not (audit == :none or
               match?({order, s} when order in [:before, :after] and is_atom(s), audit)) ->
        raise ArgumentError, "verb #{inspect(verb)} has a malformed audit #{inspect(audit)}"

      true ->
        :ok
    end
  end

  @table Map.new(@rows, fn {verb, fields} ->
           {verb,
            %{
              verb: verb,
              action: Keyword.fetch!(fields, :action),
              scope: Keyword.get(fields, :scope),
              audit: Keyword.get(fields, :audit, :none),
              audit_name: Keyword.get(fields, :audit_name),
              target: Keyword.get(fields, :target, [])
            }}
         end)

  @sources @rows
           |> Enum.flat_map(fn {_verb, fields} ->
             case Keyword.get(fields, :audit) do
               {_order, source} -> [source]
               _none -> []
             end
           end)
           |> Enum.uniq()
           |> Enum.sort()

  @doc "Every row, in declaration order."
  @spec all() :: [t()]
  def all, do: Enum.map(@rows, fn {verb, _} -> @table[verb] end)

  @spec fetch(atom()) :: {:ok, t()} | :error
  def fetch(verb) when is_atom(verb), do: Map.fetch(@table, verb)

  @doc "The row named `verb`; raises when no row is declared, which is what makes a wire name without a row a compile error."
  @spec fetch!(atom()) :: t()
  def fetch!(verb) when is_atom(verb) do
    case Map.fetch(@table, verb) do
      {:ok, row} ->
        row

      :error ->
        raise ArgumentError,
              "no operator verb #{inspect(verb)} is declared in #{inspect(__MODULE__)}"
    end
  end

  @doc "Every audit source a row declares; the durable sink subscribes to all of them."
  @spec audit_sources() :: [atom()]
  def audit_sources, do: @sources
end
