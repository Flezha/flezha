defmodule Flezha.Service.CliManager do
  @moduledoc """
  The RPC surface `flz` calls.

  Every name here is built into an RPC expression string by `cli/src`, so the
  set of names, arities and defaults is a wire contract: `flz` resolves them
  at runtime and the compiler cannot see the call. The bodies live in
  `Flezha.Service.Cli.*`, one module per verb family.
  """

  alias Flezha.Service.Cli

  defdelegate audit_log_render_list(arg1), to: Cli.Audit
  defdelegate audit_log_render_read(arg1, arg2, arg3 \\ []), to: Cli.Audit
  defdelegate audit_log_render_rotate(arg1), to: Cli.Audit
  defdelegate audit_log_render_stats(arg1), to: Cli.Audit
  defdelegate audit_log_render_tail(arg1, arg2 \\ []), to: Cli.Audit
  defdelegate authz_render_bootstrap(arg1, arg2 \\ []), to: Cli.Authz
  defdelegate authz_render_namespace_create(arg1, arg2, arg3 \\ []), to: Cli.Authz
  defdelegate authz_render_namespace_delete(arg1, arg2 \\ []), to: Cli.Authz
  defdelegate authz_render_namespace_set_quota(arg1, arg2, arg3 \\ []), to: Cli.Authz
  defdelegate authz_render_namespace_show(arg1, arg2 \\ []), to: Cli.Authz
  defdelegate authz_render_role_bind(arg1, arg2, arg3, arg4 \\ []), to: Cli.Authz
  defdelegate authz_render_role_list(arg1 \\ []), to: Cli.Authz
  defdelegate authz_render_role_unbind(arg1, arg2 \\ []), to: Cli.Authz
  defdelegate authz_render_token_issue(arg1, arg2, arg3, arg4, arg5 \\ []), to: Cli.Authz
  defdelegate authz_render_token_list(arg1 \\ []), to: Cli.Authz
  defdelegate authz_render_token_revoke(arg1, arg2 \\ []), to: Cli.Authz
  defdelegate authz_render_whoami(arg1 \\ []), to: Cli.Authz
  defdelegate authz_whoami(arg1 \\ []), to: Cli.Authz
  defdelegate boot_await(arg1 \\ 60_000), to: Cli.Boot
  defdelegate boot_status(), to: Cli.Boot
  defdelegate build_mem_top_snapshot(), to: Cli.MemTop
  defdelegate cluster_status(arg1, arg2 \\ "text"), to: Cli.Listing
  defdelegate controller_mem_top(arg1 \\ "text"), to: Cli.MemTop
  defdelegate controller_status(arg1 \\ "text"), to: Cli.Controller
  defdelegate controller_stop(arg1 \\ []), to: Cli.Controller
  defdelegate controller_stop_progress_handler(arg1, arg2, arg3, arg4), to: Cli.Controller
  defdelegate disable_plugin(arg1, arg2 \\ []), to: Cli.Plugin
  defdelegate dlq_drop(arg1, arg2 \\ []), to: Cli.Dlq
  defdelegate dlq_list(arg1 \\ ""), to: Cli.Dlq
  defdelegate dlq_replay(arg1, arg2 \\ []), to: Cli.Dlq
  defdelegate dlq_show(arg1), to: Cli.Dlq
  defdelegate enable_plugin(arg1, arg2, arg3, arg4), to: Cli.Plugin
  defdelegate entity_state(arg1, arg2), to: Cli.Operative
  defdelegate extract_subscriptions(arg1, arg2), to: Cli.Plugin
  defdelegate group_partition_rows(arg1), to: Cli.Listing
  defdelegate install_plugin(arg1, arg2), to: Cli.Plugin
  defdelegate installed_plugin?(arg1), to: Cli.Plugin
  defdelegate installed_plugin_info(arg1), to: Cli.Plugin
  defdelegate installer_disable_plugin(arg1, arg2), to: Cli.Plugin
  defdelegate installer_enable_plugin(arg1, arg2), to: Cli.Plugin
  defdelegate list_installed_plugins(arg1 \\ "text"), to: Cli.Plugin
  defdelegate list_namespaces(arg1 \\ "text"), to: Cli.Listing
  defdelegate list_operatives(arg1, arg2 \\ "text", arg3 \\ []), to: Cli.Listing
  defdelegate list_plugins(), to: Cli.Plugin
  defdelegate list_runners(arg1, arg2 \\ "text"), to: Cli.Listing
  defdelegate local_recovery_snapshot(arg1), to: Cli.Listing
  defdelegate namespace_create(arg1, arg2, arg3 \\ []), to: Cli.Authz
  defdelegate observability_disable_source(arg1, arg2 \\ []), to: Cli.Observe
  defdelegate observability_enable_source(arg1, arg2 \\ []), to: Cli.Observe
  defdelegate observability_list_sources(arg1 \\ []), to: Cli.Observe
  defdelegate observability_render_disable(arg1), to: Cli.Observe
  defdelegate observability_render_enable(arg1), to: Cli.Observe
  defdelegate observability_render_sinks(arg1), to: Cli.Observe
  defdelegate observability_render_sources(arg1), to: Cli.Observe
  defdelegate observability_render_tail(arg1, arg2, arg3 \\ []), to: Cli.Observe
  defdelegate operative_info(arg1, arg2 \\ "text"), to: Cli.Info
  defdelegate operative_listen(arg1, arg2, arg3), to: Cli.Operative
  defdelegate operative_replay(arg1, arg2, arg3), to: Cli.State
  defdelegate operative_rewind(arg1, arg2, arg3), to: Cli.Operative
  defdelegate operative_scale(arg1, arg2, arg3, arg4 \\ "{}"), to: Cli.Operative
  defdelegate operative_send(arg1, arg2, arg3, arg4), to: Cli.Operative

  defdelegate operative_start(arg1, arg2, arg3, arg4, arg5 \\ [], arg6 \\ "[]", arg7 \\ "stream"),
    to: Cli.Operative

  defdelegate operative_stop(arg1, arg2, arg3 \\ []), to: Cli.Operative
  defdelegate plugin_help(arg1), to: Cli.Plugin
  defdelegate provider_opts(arg1), to: Cli.Plugin

  defdelegate schedule_add(
                arg1,
                arg2,
                arg3,
                arg4,
                arg5 \\ "",
                arg6 \\ "",
                arg7 \\ "workflow",
                arg8 \\ ""
              ),
              to: Cli.Schedule

  defdelegate schedule_list(), to: Cli.Schedule
  defdelegate schedule_remove(arg1), to: Cli.Schedule
  defdelegate schedule_run(arg1), to: Cli.Schedule
  defdelegate state_at(arg1, arg2, arg3 \\ "text"), to: Cli.State
  defdelegate state_delete(arg1, arg2 \\ []), to: Cli.State
  defdelegate state_get(arg1, arg2 \\ "text", arg3 \\ []), to: Cli.State
  defdelegate state_inbox(arg1, arg2 \\ "text", arg3 \\ []), to: Cli.Journal
  defdelegate state_journal(arg1, arg2 \\ "text", arg3 \\ []), to: Cli.Journal
  defdelegate state_list(arg1, arg2 \\ "text", arg3 \\ []), to: Cli.State
  defdelegate state_outbox(arg1, arg2 \\ []), to: Cli.State
  defdelegate state_retention(arg1, arg2), to: Cli.State
  defdelegate state_rotate(arg1, arg2 \\ []), to: Cli.State
  defdelegate trigger_to_subscription(arg1), to: Cli.Plugin
  defdelegate uninstall_plugin(arg1, arg2), to: Cli.Plugin
  defdelegate upgrade_plugin(arg1, arg2, arg3), to: Cli.Plugin
end
