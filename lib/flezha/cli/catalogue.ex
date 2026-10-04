defmodule Flezha.Service do
  @moduledoc """
  The published consumer surface as data: which modules are on it, and the
  catalogue of the operations they publish.

  Every operation is declared once, where its function binds its row of
  `Flezha.AuthZ.Verbs` (`Flezha.AuthZ.Verb`): its positional arguments before
  the caller context, in the vocabulary plugin options use, whether it answers a
  subscription, and its description, the first sentence of its `@doc`. The
  catalogue is built from those declarations and from the rows, and is written
  nowhere else by hand. `Flezha.Service.Admin` resolves a name through it, and
  `mix flezha.catalogue` writes the copy `flz` embeds.
  """

  alias Flezha.AuthZ.Verbs

  @modules [
    Flezha.Service.AuditLog,
    Flezha.Service.Controller,
    Flezha.Service.Dlq,
    Flezha.Service.Fleet,
    Flezha.Service.Namespace,
    Flezha.Service.Observability,
    Flezha.Service.Operator,
    Flezha.Service.PluginManager,
    Flezha.Service.Schedule,
    Flezha.Service.Steps
  ]

  @type arg :: {atom(), keyword()}
  @type entry :: %{
          name: String.t(),
          module: module(),
          function: atom(),
          verb: atom(),
          action: atom(),
          args: [arg()],
          doc: String.t() | nil,
          stream: boolean(),
          remote: boolean()
        }

  @doc "The modules of the published consumer surface."
  @spec modules() :: [module()]
  def modules, do: @modules

  @doc "Every published operation, ordered by name."
  @spec catalogue() :: [entry()]
  def catalogue do
    @modules
    |> Enum.flat_map(&entries/1)
    |> Enum.sort_by(& &1.name)
  end

  @doc """
  The entry named `name` (`"<Module>.<function>"`), or `:error`. Never mints an
  atom for a name nobody declared.
  """
  @spec lookup(String.t()) :: {:ok, entry()} | :error
  def lookup(name) when is_binary(name) do
    with [module_name, function_name] when module_name != "" and function_name != "" <-
           String.split(name, ".", parts: 2),
         {:ok, module} <- existing_atom("Elixir.Flezha.Service." <> module_name),
         true <- module in @modules,
         {:ok, function} <- existing_atom(function_name),
         %{} = entry <- Enum.find(entries(module), &(&1.function == function)) do
      {:ok, entry}
    else
      _ -> :error
    end
  end

  # A function with defaulted arguments binds once per arity; it is one
  # operation, described by its longest form.
  defp entries(module) do
    module.__verbs__()
    |> Enum.group_by(fn {function, _arity, _verb, _described} -> function end)
    |> Enum.map(fn {function, bindings} ->
      {_function, _arity, verb, described} = Enum.max_by(bindings, &elem(&1, 1))
      row = Verbs.fetch!(verb)

      %{
        name: "#{module |> Module.split() |> List.last()}.#{function}",
        module: module,
        function: function,
        verb: verb,
        action: row.action,
        args: Enum.map(described[:args] || [], &required/1),
        doc: described[:doc],
        stream: described[:stream],
        remote: row.remote
      }
    end)
  end

  # An argument is required unless its row gives a default.
  defp required({name, row}),
    do: {name, Keyword.put_new(row, :required, not Keyword.has_key?(row, :default))}

  defp existing_atom(text) do
    {:ok, String.to_existing_atom(text)}
  rescue
    ArgumentError -> :error
  end
end
