defmodule Flezha.Service.CliManager do
  @moduledoc """
  The RPC surface `flz` calls, as the wire binding of `Flezha.AuthZ.Verbs`.

  Every name here is built into an RPC expression string by `cli/src`, so the
  set of names, arities and defaults is a wire contract: `flz` resolves them
  at runtime and the compiler cannot see the call. Every verb ends with its
  options list, which is where the caller's credential rides; the row it binds
  says what it is authorized as, where, and how it is audited, and the body
  lives in `Flezha.Service.Cli.*`, one module per verb family, taking the bound
  call as its last argument. A name with no row does not compile, and a public
  function that is not a `verb` line does not compile either.
  """

  alias Flezha.Service.Cli

  use Flezha.AuthZ.Verb,
    identity: &Cli.Context.cli_context/1,
    refuse: &Cli.Context.refuse!/2,
    sealed: true

  verb audit_log_render_list(format, opts \\ []), to: Cli.Audit, verb: :audit_list
  verb audit_log_render_read(file, format, opts \\ []), to: Cli.Audit, verb: :audit_read
  verb audit_log_render_rotate(format, opts \\ []), to: Cli.Audit, verb: :audit_rotate
  verb audit_log_render_stats(format, opts \\ []), to: Cli.Audit, verb: :audit_stats
  verb audit_log_render_tail(format, opts \\ []), to: Cli.Audit, verb: :audit_tail
  verb authz_render_bootstrap(token, opts \\ []), to: Cli.Authz, verb: :authz_bootstrap

  verb authz_render_namespace_create(name, quotas_json, opts \\ []),
    to: Cli.Authz,
    verb: :namespace_create

  verb authz_render_namespace_delete(name, opts \\ []), to: Cli.Authz, verb: :namespace_delete

  verb authz_render_namespace_set_quota(name, quotas_json, opts \\ []),
    to: Cli.Authz,
    verb: :namespace_set_quota

  verb authz_render_namespace_show(name, opts \\ []), to: Cli.Authz, verb: :namespace_show

  verb authz_render_role_bind(principal, role, namespace, opts \\ []),
    to: Cli.Authz,
    verb: :role_bind

  verb authz_render_role_list(opts \\ []), to: Cli.Authz, verb: :role_list

  verb authz_render_role_unbind(principal, role \\ "", opts \\ []),
    to: Cli.Authz,
    verb: :role_unbind,
    row: &Cli.Authz.binding_row/1

  verb authz_render_token_issue(principal, namespace, role, opts \\ []),
    to: Cli.Authz,
    verb: :token_issue

  verb authz_render_token_list(opts \\ []), to: Cli.Authz, verb: :token_list

  verb authz_render_token_revoke(token_id, opts \\ []),
    to: Cli.Authz,
    verb: :token_revoke,
    row: &Cli.Authz.token_row/1

  verb authz_render_whoami(opts \\ []), to: Cli.Authz, verb: :authz_whoami
  verb authz_whoami(opts \\ []), to: Cli.Authz
  verb boot_await(timeout_ms \\ 60_000, opts \\ []), to: Cli.Boot
  verb boot_status(opts \\ []), to: Cli.Boot
  verb build_mem_top_snapshot(), to: Cli.MemTop
  verb cluster_status(namespace, format \\ "text", opts \\ []), to: Cli.Listing
  verb controller_mem_top(format \\ "text", opts \\ []), to: Cli.MemTop
  verb controller_status(format \\ "text", opts \\ []), to: Cli.Controller
  verb controller_stop(opts \\ []), to: Cli.Controller

  verb disable_plugin(plugin_id, opts \\ []),
    to: Cli.Plugin,
    verb: :plugin_disable,
    row: &Cli.Plugin.plugin_row/1

  verb dlq_drop(entry_ref, opts \\ []), to: Cli.Dlq, row: &Cli.Dlq.locate/1
  verb dlq_list(operator_class \\ "", opts \\ []), to: Cli.Dlq, row: &Cli.Dlq.class_row/1
  verb dlq_replay(entry_ref, opts \\ []), to: Cli.Dlq, row: &Cli.Dlq.locate/1
  verb dlq_show(entry_ref, opts \\ []), to: Cli.Dlq, row: &Cli.Dlq.locate/1

  verb enable_plugin(plugin_id, provider_id, flags_json, opts_json, opts \\ []),
    to: Cli.Plugin,
    verb: :plugin_enable,
    row: &Cli.Plugin.enable_row/1

  verb cell_state(namespace, operator_id, opts \\ []), to: Cli.Operator
  verb group_partition_rows(rows), to: Cli.Listing
  verb install_plugin(path, opts \\ []), to: Cli.Plugin
  verb installed_plugin?(plugin_id, opts \\ []), to: Cli.Plugin, verb: :plugin_info
  verb installed_plugin_info(plugin_id, opts \\ []), to: Cli.Plugin, verb: :plugin_info
  verb installer_disable_plugin(plugin_id, opts \\ []), to: Cli.Plugin
  verb installer_enable_plugin(plugin_id, opts \\ []), to: Cli.Plugin
  verb list_installed_plugins(format \\ "text", opts \\ []), to: Cli.Plugin
  verb list_namespaces(format \\ "text", opts \\ []), to: Cli.Listing
  verb list_operators(namespace, format \\ "text", opts \\ []), to: Cli.Listing
  verb list_plugins(opts \\ []), to: Cli.Plugin, verb: :plugin_list
  verb list_runners(namespace, format \\ "text", opts \\ []), to: Cli.Listing

  verb observability_render_disable(source, opts \\ []),
    to: Cli.Observe,
    verb: :observability_disable

  verb observability_render_enable(source, opts \\ []),
    to: Cli.Observe,
    verb: :observability_enable

  verb observability_render_sinks(format, opts \\ []),
    to: Cli.Observe,
    verb: :observability_sinks

  verb observability_render_sources(format, opts \\ []),
    to: Cli.Observe,
    verb: :observability_sources

  verb observability_render_tail(source, format, opts \\ []),
    to: Cli.Observe,
    verb: :observability_tail

  verb operator_info(namespace, format \\ "text", opts \\ []), to: Cli.Info

  verb operator_listen(namespace, operator_id, direction, format, opts \\ []),
    to: Cli.Operator

  verb operator_replay(namespace, path, from, opts \\ []),
    to: Cli.State,
    row: &Cli.State.replay_row/1

  verb operator_rewind(namespace, operator_id, to_iter, opts \\ []),
    to: Cli.Operator

  verb operator_scale(namespace, group_id, target, opts_json \\ "{}", opts \\ []),
    to: Cli.Operator

  verb operator_send(namespace, operator_id, message, channel, opts \\ []),
    to: Cli.Operator

  verb operator_start(namespace, factory_id, init_values, format, opts_json \\ "{}", opts \\ []),
    to: Cli.Operator,
    target: &Flezha.Service.OperatorManager.start_target/2

  verb operator_stop(namespace, operator_id, opts \\ []), to: Cli.Operator
  verb plugin_help(provider_id, opts \\ []), to: Cli.Plugin, verb: :plugin_info
  verb schedule_add(name, opts \\ []), to: Cli.Schedule
  verb schedule_list(opts \\ []), to: Cli.Schedule
  verb schedule_remove(name, opts \\ []), to: Cli.Schedule
  verb schedule_run(name, opts \\ []), to: Cli.Schedule, row: &Cli.Schedule.locate/1

  verb state_at(path, target, format \\ "text", opts \\ []),
    to: Cli.State,
    row: &Cli.State.at_row/1

  verb state_delete(path, opts \\ []), to: Cli.State, row: &Cli.State.split_row/1
  verb state_get(path, format \\ "text", opts \\ []), to: Cli.State, row: &Cli.State.live_row/1

  verb state_inbox(instance_id, format \\ "text", opts \\ []),
    to: Cli.Journal,
    row: &Cli.Journal.instance_row/1

  verb state_journal(instance_id, format \\ "text", opts \\ []),
    to: Cli.Journal,
    row: &Cli.Journal.instance_row/1

  verb state_list(operator_class, format \\ "text", opts \\ []),
    to: Cli.State,
    row: &Cli.State.class_row/1

  verb state_outbox(path, opts \\ []), to: Cli.State, row: &Cli.State.outbox_row/1
  verb state_retention(operator_class, hours, opts \\ []), to: Cli.State
  verb state_rotate(path, opts \\ []), to: Cli.State, row: &Cli.State.rotate_row/1
  verb uninstall_plugin(plugin_id, opts \\ []), to: Cli.Plugin
  verb upgrade_plugin(plugin_id, path, opts \\ []), to: Cli.Plugin
end
