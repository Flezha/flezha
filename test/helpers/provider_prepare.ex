defmodule FlezhaTest.Helper.ProviderPrepare do
  @moduledoc """
  Preparation and activation as `Flezha.Plugin.Extension.Table` performs them,
  for a test that drives one provider rather than a whole operative.

  The table does two things before a provider's `init_provider/1` runs: it
  merges the host's identity and it normalises the options through the
  provider's declaration. A test that skips either is testing a shape the
  system never produces, so both live here rather than in each test file.
  """

  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  @doc """
  Prepare `opts` for `provider`: normalise through the declaration, then run
  `init_provider/1`. Answers what the table would store, or the refusal.
  """
  @spec prepare(module(), map()) :: {:ok, map()} | {:error, term()}
  def prepare(provider, opts) do
    with {:ok, normalised} <- ProviderOptions.normalise_opts(provider, with_host_identity(opts)),
         do: provider.init_provider(normalised)
  end

  # What `Table.with_host_identity/3` merges: the caller stands in for the host
  # operative, which is what a provider's worker is handed.
  defp with_host_identity(opts) do
    Map.merge(
      %{
        overwatcher_pid: self(),
        source_namespace: :default,
        source_operative_id: "test-operative",
        source_plugin_id: Map.get(opts, :plugin_id)
      },
      opts
    )
  end

  @doc """
  Start `provider`'s declared workers from prepared `opts`, exactly as
  activation does: through `Table.Workers`, which is the production path, so a
  test cannot drift from what the table actually starts. Answers the first
  worker's pid, which is the one the table tracks as the plugin's own.
  """
  @spec activate(module(), map()) :: {:ok, pid()} | {:error, term()}
  def activate(provider, opts) do
    plugin_id = Map.get(opts, :plugin_id) || "provider-prepare"
    table = %{entries: %{}, monitors: %{}}

    case Workers.start(table, plugin_id, provider, opts, & &1.()) do
      {started, :ok} -> {:ok, Entry.owned_pid(started.entries[plugin_id])}
      {_table, {:error, _} = err} -> err
    end
  end

  @doc "Prepare and activate in one call, answering the prepared opts and the worker."
  @spec prepare_and_activate(module(), map()) :: {:ok, map(), pid()} | {:error, term()}
  def prepare_and_activate(provider, opts) do
    with {:ok, prepared} <- prepare(provider, opts),
         {:ok, pid} <- activate(provider, prepared),
         do: {:ok, prepared, pid}
  end
end
