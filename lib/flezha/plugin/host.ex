defmodule Flezha.Plugin.Host do
  @moduledoc """
  Macro used by a module that exposes extension points (e.g.
  `Flezha.Operative.Overwatch`).

      use Flezha.Plugin.Host, extensions: [
        {:on_input, :transform, Flezha.Plugin.Extension.Ctx.IO},
        {:on_interval, :timer,  Flezha.Plugin.Extension.Ctx.Tick},
        ...
      ]

  Each catalog entry is `{name :: atom, kind :: kind(), ctx_module :: module}`.
  Bit indices are assigned from declaration order and used by the
  active-mask fast path in `Flezha.Plugin.Extension.Table`.
  """

  @valid_kinds [:fold, :transform, :fanout, :request, :timer]

  defmacro __using__(opts) do
    extensions_ast = Keyword.fetch!(opts, :extensions)
    extensions = extensions_from_ast(extensions_ast, __CALLER__)
    catalog = build_catalog(extensions, __CALLER__)

    quote do
      @extensions unquote(Macro.escape(extensions))
      @extensions_catalog unquote(Macro.escape(catalog))

      @doc false
      def __extensions__, do: @extensions

      @doc false
      def __extensions_catalog__, do: @extensions_catalog
    end
  end

  defp extensions_from_ast(list, caller) when is_list(list) do
    Enum.map(list, fn
      {:{}, _, [name, kind, ctx_ast]} ->
        {name, kind, Macro.expand(ctx_ast, caller)}

      {name, kind, ctx_ast} ->
        {name, kind, Macro.expand(ctx_ast, caller)}

      other ->
        raise CompileError,
          file: caller.file,
          line: caller.line,
          description: "Invalid extension entry in AST: #{inspect(other)}"
    end)
  end

  @doc false
  def build_catalog(extensions, caller) when is_list(extensions) do
    extensions
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn {entry, bit_index}, acc ->
      case entry do
        {name, kind, ctx_module}
        when is_atom(name) and kind in @valid_kinds and is_atom(ctx_module) ->
          if Map.has_key?(acc, name) do
            raise CompileError,
              file: caller.file,
              line: caller.line,
              description: "Duplicate extension name: #{inspect(name)}"
          end

          Map.put(acc, name, %{
            name: name,
            kind: kind,
            ctx_module: ctx_module,
            bit_index: bit_index
          })

        other ->
          raise CompileError,
            file: caller.file,
            line: caller.line,
            description:
              "Invalid extension catalog entry: #{inspect(other)}. " <>
                "Expected {name, kind, ctx_module} with kind in #{inspect(@valid_kinds)}."
      end
    end)
  end

  @doc "Returns the set of valid dispatch kinds."
  def valid_kinds, do: @valid_kinds
end
