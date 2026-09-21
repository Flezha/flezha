defmodule Flezha.Docs.Lexer do
  @moduledoc false

  # The lexer behind the fence names `mix docs` registers in `mix.exs`. Every
  # block is read by makeup_syntect's grammar for `:language`, and two languages
  # take one more step.
  #
  # Shell. A block that shows a prompt is a transcript: its commands, a `$ `
  # line and every line a trailing `\` continues, are coloured, its output is
  # left as written, and the prompt stays unselectable as ExDoc keeps it.
  #
  # Ruby. The grammar marks a class name three ways by where it stands: being
  # defined, as a parent class and as a reference. ExDoc's theme colours the
  # last two like an operator and like a keyword argument, so a class name gets
  # the colour of the first wherever it appears.

  @shell "Bourne Again Shell (bash)"

  def lex(text, opts) do
    case Keyword.fetch!(opts, :language) do
      @shell -> shell(text)
      "Ruby" -> text |> syntect(opts) |> Enum.map(&class_name/1)
      _language -> syntect(text, opts)
    end
  end

  defp shell(text) do
    lines = String.split(text, "\n")

    if Enum.any?(lines, &String.starts_with?(&1, "$ ")),
      do: transcript(lines),
      else: bash(text)
  end

  defp transcript(lines) do
    {tokens, _continued} =
      Enum.flat_map_reduce(lines, false, fn
        "$ " <> command, _continued ->
          prompt = {:generic_prompt, %{selectable: false}, "$ "}
          {[prompt | bash(command <> "\n")], continues?(command)}

        line, true ->
          {bash(line <> "\n"), continues?(line)}

        line, false ->
          {[{:text, %{}, line <> "\n"}], false}
      end)

    tokens
  end

  defp continues?(line), do: line |> String.trim_trailing() |> String.ends_with?("\\")

  defp bash(code), do: syntect(code, language: @shell)

  defp syntect(code, opts), do: MakeupSyntect.Lexer.lex(code, opts)

  defp class_name({:name_cell, meta, value}), do: {:name_class, meta, value}

  defp class_name({:name_constant, meta, value} = token) do
    if value |> List.wrap() |> IO.iodata_to_binary() |> String.match?(~r/^[A-Z]/),
      do: {:name_class, meta, value},
      else: token
  end

  defp class_name(token), do: token
end
