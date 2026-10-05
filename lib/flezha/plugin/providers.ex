defmodule Flezha.Plugin.Providers do
  @moduledoc """
  The provider table: every provider id the system can enable, and the module
  behind it.

  A first-party provider is a row of `@first_party` and nothing else — no
  configuration entry, no supervisor veneer, no second list to keep in step. A
  plugin package's providers are added through `add/2` by the boot loader, once,
  before the supervision tree is built, and are resolved by `flz plugin enable`,
  `--plugins` decoding and `flz plugin list` exactly as a first-party row is.
  A row is never replaced or removed while the controller runs: a package change
  takes effect at the next boot.

  Added rows live in a `:persistent_term` rather than the application
  environment: they are written once per boot and read on every enable and every
  spec decode.
  """

  alias Flezha.Config.Durations
  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Provider
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour

  @installed_key {__MODULE__, :installed}

  @first_party %{
    "relay_postgres" => Provider.Postgres,
    "relay_postgres_poll" => Provider.PostgresPoll,
    "relay_websocket" => Provider.Websocket,
    "relay_http_poll" => Provider.HttpPoll,
    "relay_http_push" => Provider.HttpPush,
    "relay_file_input" => Provider.File.Source,
    "relay_file_output" => Provider.File.Sink,
    "relay_beam_msg" => Provider.BeamMsgForward,
    "relay_stdout" => Provider.StdOut,
    "relay_io_filter" => Provider.Filter,
    "monitored_io_forward" => Provider.MonitoredIOForward,
    "relay_forward" => Provider.RelayForward,
    # An `:on_message` provider that forwards a `{:flezha_request, key, payload,
    # reply_to, ref}` envelope to the SDK-side relay. Attach at relay
    # start with a hook keyed on the handler key, e.g.
    # `["on_message", {"key": "fetch_init_state"}]`.
    "relay_message_forward" => Provider.MessageForward,
    # The first-party controller plugin: every hook event, POSTed as JSON.
    "hook_webhook" => Provider.HookWebhook
  }

  @doc "Every provider id to its module, first-party rows and installed ones."
  @spec all() :: %{binary() => module()}
  def all(), do: Map.merge(@first_party, installed())

  @doc "The rows plugin packages added at this boot."
  @spec installed() :: %{binary() => module()}
  def installed(), do: :persistent_term.get(@installed_key, %{})

  @doc "The rows the release itself provides."
  @spec first_party() :: %{binary() => module()}
  def first_party(), do: @first_party

  @doc "The module behind `provider_id`, or `:error`."
  @spec fetch(binary()) :: {:ok, module()} | :error
  def fetch(provider_id) when is_binary(provider_id), do: Map.fetch(all(), provider_id)

  @doc """
  The module behind `provider_id`, or why there is none: a package that
  declares it but did not load names itself and why, rather than the provider
  being unknown.
  """
  @spec resolve(binary()) :: {:ok, module()} | {:error, term()}
  def resolve(provider_id) when is_binary(provider_id) do
    with :error <- fetch(provider_id),
         do: {:error, Installer.unavailable(provider_id) || {:unknown_provider, provider_id}}
  end

  @doc "Every provider row, by id: its module and the plugin type it declares."
  @spec describe_all() :: [map()]
  def describe_all do
    for {provider_id, module} <- Enum.sort(all()) do
      %{id: provider_id, module: inspect(module), type: plugin_type(module)}
    end
  end

  @doc """
  What a caller is shown about one provider: its help text, its plugin type,
  and the hooks it supports.
  """
  @spec help(binary()) :: {:ok, map()} | {:error, term()}
  def help(provider_id) do
    with {:ok, module} <- resolve(provider_id) do
      {:ok,
       %{
         provider: provider_id,
         help: provider_help(module, provider_id),
         type: plugin_type(module),
         hooks: module.__provider__().extensions
       }}
    end
  rescue
    e -> {:error, {:plugin_help_failed, provider_id, Exception.message(e)}}
  end

  @doc """
  The controllers of `peers` whose provider table has no `provider_id`, or that
  did not answer. A provider comes from a package installed per controller, so
  another controller holding a namespace's hub may lack it.
  """
  @spec lacking([node()], binary()) :: [node()]
  def lacking(peers, provider_id) do
    answers =
      :erpc.multicall(
        peers,
        __MODULE__,
        :fetch,
        [provider_id],
        Durations.get(:plugin_peer_check_ms)
      )

    for {peer, answer} <- Enum.zip(peers, answers), not match?({:ok, {:ok, _}}, answer), do: peer
  end

  defp provider_help(module, provider_id) do
    module.help(provider_id)
  rescue
    UndefinedFunctionError -> nil
  end

  defp plugin_type(module) do
    module.plugin_type()
  rescue
    _ -> nil
  end

  @doc "Register a plugin package's provider as a row, refusing an id that already is one."
  @spec add(binary(), module()) :: :ok | {:error, {:provider_id_taken, binary(), module()}}
  def add(provider_id, module) when is_binary(provider_id) and is_atom(module) do
    case fetch(provider_id) do
      {:ok, holder} ->
        {:error, {:provider_id_taken, provider_id, holder}}

      :error ->
        :persistent_term.put(@installed_key, Map.put(installed(), provider_id, module))
    end
  end

  @doc "Whether `module` answers a provider declaration of its own."
  @spec provider?(module()) :: boolean()
  def provider?(module) when is_atom(module) do
    match?(%ProviderBehaviour.Decl{module: ^module}, module.__provider__())
  rescue
    _ -> false
  end

  @doc "Drop every added row. For tests that load packages in one VM."
  @spec reset_installed() :: :ok
  def reset_installed(), do: :persistent_term.put(@installed_key, %{})

  @doc """
  The worker families to supervise: one per provider row that declares a worker,
  and the shared plugins' copies. Read by the application's supervision tree, so
  a provider that gains a worker gains its supervision from the same declaration.

  Sorted, because a supervision tree's child order is part of what the boot
  contract pins and a map's iteration order is not stable across compilations.
  """
  @spec worker_families() :: [module()]
  def worker_families() do
    providers =
      for {_id, module} <- Enum.sort(all()),
          Code.ensure_loaded?(module),
          module.__provider__().worker,
          uniq: true,
          do: WorkerSup.family(module)

    providers ++ [WorkerSup.family(Flezha.Plugin.Shared)]
  end
end
