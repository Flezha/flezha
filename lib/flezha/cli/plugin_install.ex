defmodule Flezha.Plugin.Installer.Command do
  @moduledoc """
  `flz plugin install | uninstall | sync`: the verbs that place packages in the
  plugin root. They run in a VM of their own (`flzvm eval`), never inside a
  running controller, and act on the plugin root alone, so no controller
  authorizes them.
  """

  alias Flezha.Error
  alias Flezha.Plugin.Installer
  alias Flezha.Service.Cli.Context

  @doc """
  Run `verb`, print the answer and the absolute plugin root, and halt with the
  exit code of the answer's class.
  """
  @spec eval(binary(), [binary()], keyword()) :: no_return()
  def eval(verb, args, opts) do
    Logger.configure(level: :warning)
    {:ok, _} = Application.ensure_all_started([:crypto, :public_key, :ssl])
    format = Context.format(opts)

    answer =
      case {verb, args} do
        {"install", [source]} -> Installer.install(source)
        {"uninstall", [id]} -> Installer.uninstall(id)
        {"sync", []} -> Installer.sync()
      end

    case answer do
      {:ok, done} ->
        IO.puts(placed(verb, done, format))
        System.halt(0)

      {:error, reason} ->
        reason |> Error.from_reason(nil) |> Context.render!(opts)
    end
  end

  defp placed(_verb, done, "json"), do: Jason.encode!(done)

  defp placed("install", done, _text) do
    signed = if done.signed, do: "signed", else: "unsigned"

    "Installed #{done.id} #{done.version} (#{signed}) into #{done.root}.\n" <>
      "Restart the controller to load it." <>
      if(done.warning, do: "\nwarning: #{done.warning}", else: "")
  end

  defp placed("uninstall", done, _text),
    do: "Uninstalled #{done.id} from #{done.root}.\nRestart the controller to unload it."

  defp placed("sync", done, _text) do
    placed = Enum.map(done.placed, &"#{&1.id} #{&1.version}")
    removed = Enum.map(done.removed, &Path.basename/1)

    "Synced #{done.root}: placed #{names(placed)}, kept #{names(done.kept)}, " <>
      "removed #{names(removed)}.\nThe controller loads them at its next start."
  end

  defp names([]), do: "none"
  defp names(names), do: Enum.join(names, ", ")
end
