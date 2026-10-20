defmodule Flezha.Api.CliImportsTest do
  @moduledoc """
  The controller side of `flz` reaches the controller only through the door:
  every `Flezha.Service.Cli` module calls the door, the catalogue, its own
  renderers, `Flezha.Error` and the standard library, and nothing else. Read
  from each compiled module's imports, so a call the source hides behind an
  alias or a pipe is still seen.
  """
  use ExUnit.Case, async: true

  # The door and the catalogue; the error every failure is rendered from; and
  # what a caller context is built from: a verified token, and the node's own
  # bounds for how long the CLI waits on a caller.
  @allowed_modules [Flezha.Service.Admin, Flezha.Service, Flezha.Error]
  # `Flezha.Mcp.relay_tool_names/1` is the MCP surface's naming rule over the
  # entries the door answered; it reads no controller state. Neither does
  # `Flezha.Service.RelayApi.answer/1`, the one decoder of an answer the door
  # carried, which every surface maps to its medium.
  @allowed_calls [
    {Flezha.Service.CallerContext, :from_token, 2},
    {Flezha.Config.Durations, :get, 1},
    {Flezha.Mcp, :relay_tool_names, 1},
    {Flezha.Service.RelayApi, :answer, 1}
  ]

  # Encoding the JSON a renderer prints is part of rendering.
  @allowed_apps [:elixir, :stdlib, :kernel, :erts, :jason]

  @doc false
  def forbidden_calls(module, beam) do
    {:ok, {^module, [imports: imports]}} = :beam_lib.chunks(beam, [:imports])

    for {callee, _function, _arity} = call <- imports,
        not allowed?(callee, call),
        uniq: true,
        do: {module, call}
  end

  defp allowed?(callee, call) do
    callee in @allowed_modules or call in @allowed_calls or cli?(callee) or
      application(callee) in @allowed_apps
  end

  defp cli?(module), do: String.starts_with?(Atom.to_string(module), "Elixir.Flezha.Service.Cli")

  defp application(module) do
    case :application.get_application(module) do
      {:ok, app} -> app
      :undefined -> if :code.which(module) == :preloaded, do: :erts
    end
  end

  test "every CLI module reaches the controller only through the door" do
    {:ok, modules} = :application.get_key(:flezha, :modules)

    calls =
      for module <- modules,
          cli?(module),
          call <- forbidden_calls(module, :code.which(module)),
          do: call

    assert calls == [], "CLI modules call outside the door: #{inspect(calls, pretty: true)}"
  end

  test "a CLI module that reads a store is named with the call" do
    [{module, beam}] =
      Code.compile_string("""
      defmodule Flezha.Service.Cli.PlantedReadsAStore do
        def list, do: Flezha.Dlq.Store.list(nil)
      end
      """)

    assert forbidden_calls(module, beam) == [{module, {Flezha.Dlq.Store, :list, 1}}]
  end
end
