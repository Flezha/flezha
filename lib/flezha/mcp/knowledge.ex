defmodule Flezha.Mcp.Knowledge do
  @moduledoc """
  The tools that answer from the installed release rather than from a
  controller, so an assistant has them before any controller runs: the working
  rules with the docs map, an SDK's echo example, an error code's meaning and
  one guide page.

  `mix flezha.ai_kit` writes what they read into `priv/ai_kit/`; an error's
  meaning is read from the compiled catalogue. The rules are also the server's
  `instructions`.
  """

  alias Flezha.Error

  @languages ~w(elixir java node php python ruby rust)

  # Declared as catalogue entries are, so `Flezha.Mcp` gives them their tool
  # name, schema and argument handling as it gives an operation's.
  @declared [
    %{
      name: "Flezha.guide",
      doc:
        "Start here before working on a Flezha application: the working rules and the map " <>
          "of every guide page, each with one line on what it covers.",
      args: []
    },
    %{
      name: "Flezha.sdk_example",
      doc:
        "The current echo example of one SDK: every source file of a small runner that " <>
          "registers an operator, in the form this release's SDK takes. Read it before " <>
          "writing SDK code; never write an SDK form from memory.",
      args: [
        language: [type: :enum, values: @languages, required: true, doc: "The SDK's language"]
      ]
    },
    %{
      name: "Flezha.error",
      doc:
        "The meaning of a Flezha error code: its class, its message and the hint naming the " <>
          "next step.",
      args: [code: [type: :string, required: true, doc: "The code, for example dlq.not_found"]]
    },
    %{
      name: "Flezha.docs",
      doc:
        "One Flezha guide page as Markdown, by the file name the map in flezha_guide links, " <>
          "for example local_mode.md.",
      args: [page: [type: :string, required: true, doc: "The page's file name"]]
    }
  ]

  @entries for entry <- @declared, do: Map.merge(entry, %{audited: false, action: :public})

  @doc """
  The knowledge tools as catalogue-shaped entries: read-only, needing no
  action, and answered by `call/2` rather than a controller's door.
  """
  @spec entries() :: [map()]
  def entries, do: @entries

  @doc "The languages `flezha_sdk_example` serves."
  @spec languages() :: [String.t()]
  def languages, do: @languages

  @doc "The working rules: the server's `instructions` and the head of `flezha_guide`."
  @spec rules() :: String.t()
  def rules, do: read!("rules.md")

  @doc """
  The answer of the knowledge entry `name` to its `arguments`, in its rows'
  order: Markdown as `{:text, body}`, an error's meaning as `{:ok, json}`, or
  the refusal. It has the shape of a door.
  """
  @spec call(String.t(), [term()]) ::
          {:text, String.t()} | {:ok, String.t()} | {:error, Error.t()}
  def call("Flezha.guide", []), do: {:text, rules() <> "\n" <> read!("llms.txt")}

  def call("Flezha.sdk_example", [language]) do
    if language in @languages,
      do: {:text, read!("sdk/#{language}.md")},
      else:
        {:error,
         Error.new("mcp.unknown_language",
           language: shown(language),
           languages: Enum.join(@languages, ", ")
         )}
  end

  def call("Flezha.error", [code]) do
    case Error.catalogue()["codes"] do
      %{^code => entry} ->
        {:ok, Jason.encode!(Map.merge(%{"code" => code}, Map.drop(entry, ["reasons"])))}

      _none ->
        {:error, Error.new("mcp.unknown_code", requested: shown(code))}
    end
  end

  def call("Flezha.docs", [page]) do
    file = if is_binary(page), do: String.replace_suffix(page, ".md", "") <> ".md"

    # The page is looked up among the files written, never joined into a path
    # unseen, so no name reaches outside the folder.
    if file in pages(),
      do: {:text, read!("docs/" <> file)},
      else: {:error, Error.new("mcp.unknown_page", page: shown(page))}
  end

  defp pages do
    case File.ls(path("docs")) do
      {:ok, files} -> files
      {:error, _} -> []
    end
  end

  defp shown(value) when is_binary(value), do: value
  defp shown(value), do: inspect(value)

  defp read!(relative), do: File.read!(path(relative))

  defp path(relative), do: Application.app_dir(:flezha, Path.join("priv/ai_kit", relative))
end
