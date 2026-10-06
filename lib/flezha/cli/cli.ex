defmodule Flezha.Service.Cli do
  @moduledoc """
  The one function `flz` calls on a controller: a published operation by its
  door name, its arguments as a JSON array, and the trailing options list the
  caller's token and output format ride in.

  `call/3` builds the caller context from the token, runs the operation
  through `Flezha.Service.Admin`, and renders the answer: as the door's JSON
  under `output: "json"`, otherwise with the operation's text renderer, or a
  default text form for one that has none. A failure is rendered by
  `Flezha.Service.Cli.Context.render!/2` and ends the call with its class's exit
  code.

  An operation whose rendering is more than one answer — a subscription, a
  listener, a stop that reports its progress — has a driver instead, which
  runs the operation through the door itself and prints as it goes.
  """

  alias Flezha.Service.Admin

  alias Flezha.Service.Cli.{
    Audit,
    Authz,
    Boot,
    Context,
    Controller,
    Dlq,
    Info,
    Journal,
    Listing,
    MemTop,
    Observe,
    Relay,
    Plugin,
    Schedule,
    State
  }

  # Drivers take the arguments, the caller context and the options; renderers
  # take the answer, the arguments as written and the options, so an act that
  # answers `:ok` can still name what it acted on.
  @drivers %{
    "AuditLog.tail" => &Audit.tail/3,
    "Controller.stop" => &Controller.stop/3,
    "Observability.tail" => &Observe.tail/3,
    "Relay.call_channel" => &Relay.call_channel/3,
    "Relay.listen" => &Relay.listen/3,
    "Relay.send" => &Relay.send/3
  }

  @renderers %{
    "AuditLog.list_files" => &Audit.list_files/3,
    "AuditLog.read" => &Audit.read/3,
    "AuditLog.rotate" => &Audit.rotate/3,
    "AuditLog.stats" => &Audit.stats/3,
    "Controller.boot_await" => &Boot.await/3,
    "Controller.boot_status" => &Boot.status/3,
    "Controller.cluster_status" => &Listing.cluster/3,
    "Controller.mem_top" => &MemTop.mem_top/3,
    "Controller.status" => &Controller.status/3,
    "Namespace.bind_role" => &Authz.bind_role/3,
    "Namespace.bootstrap" => &Authz.bootstrap/3,
    "Namespace.create" => &Authz.create/3,
    "Namespace.delete" => &Authz.delete/3,
    "Namespace.get" => &Authz.get/3,
    "Namespace.issue_token" => &Authz.issue_token/3,
    "Namespace.list_roles" => &Authz.list_roles/3,
    "Namespace.revoke_token" => &Authz.revoke_token/3,
    "Namespace.set_quotas" => &Authz.set_quotas/3,
    "Namespace.tokens" => &Authz.tokens/3,
    "Namespace.unbind_role" => &Authz.unbind_role/3,
    "Namespace.whoami" => &Authz.whoami/3,
    "Dlq.drop" => &Dlq.drop/3,
    "Dlq.get" => &Dlq.show/3,
    "Dlq.list" => &Dlq.list/3,
    "Dlq.replay" => &Dlq.replay/3,
    "Fleet.installed_plugins" => &Plugin.installed/3,
    "Fleet.provider" => &Plugin.provider/3,
    "Fleet.providers" => &Plugin.providers/3,
    "Fleet.runners" => &Listing.runners/3,
    "Observability.disable_source" => &Observe.disable/3,
    "Observability.enable_source" => &Observe.enable/3,
    "Observability.list_sinks" => &Observe.sinks/3,
    "Observability.list_sources" => &Observe.sources/3,
    "Relay.api" => &Info.api/3,
    "Relay.call_channel" => &Relay.answered/3,
    "Relay.cell_state" => &Relay.cell_state/3,
    "Relay.diagnose" => &Info.diagnose/3,
    "Relay.info" => &Info.info/3,
    "Relay.list" => &Listing.relays/3,
    "Relay.list_namespaces" => &Listing.namespaces/3,
    "Relay.replay" => &Relay.replay/3,
    "Relay.send" => &Relay.delivered/3,
    "Relay.start" => &Relay.start/3,
    "Relay.stop" => &Relay.stop/3,
    "PluginManager.bind" => &Plugin.bind/3,
    "PluginManager.disable" => &Plugin.disable/3,
    "PluginManager.enable" => &Plugin.enable/3,
    "PluginManager.enabled" => &Plugin.enabled/3,
    "PluginManager.unbind" => &Plugin.unbind/3,
    "PluginManager.update" => &Plugin.update/3,
    "Schedule.add" => &Schedule.add/3,
    "Schedule.list" => &Schedule.list/3,
    "Schedule.remove" => &Schedule.remove/3,
    "Schedule.run" => &Schedule.run/3,
    "Steps.cells" => &State.cells/3,
    "Steps.delete" => &State.delete/3,
    "Steps.inbox" => &Journal.inbox/3,
    "Steps.journal" => &Journal.journal/3,
    "Steps.outbox" => &State.outbox/3,
    "Steps.rescale" => &Relay.rescale/3,
    "Steps.rewind" => &Relay.rewind/3,
    "Steps.rotate" => &State.rotate/3,
    "Steps.set_retention" => &State.set_retention/3,
    "Steps.state_get" => &State.state_get/3,
    "Steps.time_travel" => &State.time_travel/3
  }

  @doc "Run the operation `name` for the caller `opts` names and print its answer."
  @spec call(String.t(), String.t(), keyword()) :: :ok
  def call(name, args_json, opts) when is_binary(name) and is_binary(args_json) do
    context = Context.cli_context(opts)

    case Map.fetch(@drivers, name) do
      {:ok, drive} -> drive.(args_json, context, opts)
      :error -> name |> Admin.run(args_json, context, :given) |> render(name, args_json, opts)
    end

    :ok
  end

  @doc """
  Print one answer of `name`: the door's JSON or its text form, or the failure.
  A driver renders the answers it ran through the door with this.
  """
  @spec render({:ok, term()} | {:error, Flezha.Error.t()}, String.t(), String.t(), keyword()) ::
          :ok
  def render({:error, %Flezha.Error{} = error}, _name, _args_json, opts),
    do: Context.render!(error, opts)

  def render({:ok, value}, name, args_json, opts) do
    case Context.format(opts) do
      "json" -> IO.puts(Admin.json(value))
      _text -> Map.get(@renderers, name, &default_text/3).(value, written(args_json), opts)
    end

    :ok
  end

  defp written(args_json) do
    case Jason.decode(args_json) do
      {:ok, args} when is_list(args) -> args
      _unreadable -> []
    end
  end

  # An operation with no renderer of its own: a bare acknowledgement says so,
  # text is printed as it is, and anything else as indented JSON.
  defp default_text(:ok, _args, _opts), do: IO.puts("ok")
  defp default_text(text, _args, _opts) when is_binary(text), do: IO.puts(text)

  defp default_text(value, _args, _opts),
    do: value |> Admin.json() |> Jason.decode!() |> Jason.encode!(pretty: true) |> IO.puts()
end
