defmodule Flezha.Plugin.PackageSurfaceTest do
  @moduledoc """
  A plugin package names only the core modules declared as its surface. A
  connector's own modules (the poll source's SQL, the HTTP fetcher) are not on
  it: a package that needs one of their functions asks for it on a surface
  module, as the Postgres CDC package reads tables through
  `Flezha.Plugin.Provider.Postgres.Connection`.
  """
  use ExUnit.Case, async: true

  alias FlezhaTest.Helper.SourceWalk

  @surface MapSet.new([
             Flezha.Config.Backoff,
             Flezha.Config.Durations,
             Flezha.Error,
             Flezha.Mcp,
             Flezha.Plugin.Extension.Ctx,
             Flezha.Plugin.Extension.Ctx.IO,
             Flezha.Plugin.Extension.Ctx.Terminate,
             Flezha.Plugin.Host,
             Flezha.Plugin.Host.MsgId,
             Flezha.Plugin.Provider.Postgres.Connection,
             Flezha.Plugin.Provider.Worker,
             Flezha.Plugin.Provider.WorkerSup,
             Flezha.Plugin.ProviderBehaviour,
             Flezha.Plugin.ProviderBehaviour.Options,
             Flezha.Process.Backlog,
             Flezha.Process.Cspec,
             Flezha.Process.Poll,
             Flezha.Process.Timer,
             Flezha.Process.Watch,
             Flezha.Relay.Ingest.Verdict,
             Flezha.SDK.SafeCall,
             Flezha.Service,
             Flezha.Service.Admin,
             Flezha.Service.CallerContext,
             Flezha.Service.Controller,
             Flezha.Service.Listener,
             Flezha.Service.RelayApi,
             Flezha.State.Outbox
           ])

  # Fails when: a package module names a core module off the surface, written
  # out or through an alias.
  test "every core module a package names is on the package surface" do
    files = SourceWalk.files(Path.wildcard("plugins/*/lib"), [".ex"])
    assert files != [], "no package source under plugins/*/lib"

    off =
      for file <- files,
          module <- core_modules(SourceWalk.ast(file)),
          not MapSet.member?(@surface, module),
          uniq: true,
          do: "#{file}: #{inspect(module)}"

    assert off == [], "core modules off the package surface:\n" <> Enum.join(off, "\n")
  end

  defp core_modules(ast) do
    aliases = aliases_of(ast)

    {_, named} =
      Macro.prewalk(ast, [], fn
        {:__aliases__, _, [head | rest]} = node, acc when is_atom(head) ->
          {node, [Module.concat([Map.get(aliases, head, head) | rest]) | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.filter(named, &(hd(Module.split(&1)) == "Flezha"))
  end

  defp aliases_of(ast) do
    {_, aliases} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, [{:__aliases__, _, parts}]} = node, acc ->
          {node, Map.put(acc, List.last(parts), Module.concat(parts))}

        {:alias, _, [{:__aliases__, _, parts}, [as: {:__aliases__, _, [as]}]]} = node, acc ->
          {node, Map.put(acc, as, Module.concat(parts))}

        node, acc ->
          {node, acc}
      end)

    aliases
  end
end
