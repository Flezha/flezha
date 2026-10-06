defmodule Flezha.Mcp.Knowledge.Command do
  @moduledoc """
  `flz template list | create`: the templates the installed release carries,
  read in a VM of its own (`flzvm eval`) by the reader `flezha_new_project`
  answers from, so no controller is needed. `create` prints the template as
  one JSON line, and `flz` writes the folder or the tar stream itself.
  """

  alias Flezha.Mcp.Knowledge
  alias Flezha.Service.Cli.{Context, Listing}

  @doc """
  Print `verb`'s answer and halt with the exit code of its class.
  """
  @spec eval(binary(), [binary()], keyword()) :: no_return()
  def eval("list", [], opts) do
    case Context.format(opts) do
      "json" -> IO.puts(Jason.encode!(Knowledge.templates()))
      _text -> Listing.templates(Knowledge.templates(), [], opts)
    end

    System.halt(0)
  end

  def eval("create", [language, example], opts) do
    case Knowledge.template(language, example) do
      {:ok, template} ->
        IO.puts(Jason.encode!(template))
        System.halt(0)

      {:error, error} ->
        Context.render!(error, opts)
    end
  end
end
