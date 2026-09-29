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

  alias Flezha.Plugin.Provider
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour

  @installed_key {__MODULE__, :installed}

  @first_party %{
    "operator_kafka_producer" => Provider.Kafka.Producer,
    "operator_postgres" => Provider.Postgres,
    "operator_websocket" => Provider.Websocket,
    "operator_http_poll" => Provider.HttpPoll,
    "operator_file_source" => Provider.File.Source,
    "operator_file_sink" => Provider.File.Sink,
    "operator_beam_msg" => Provider.BeamMsgForward,
    "operator_stdout" => Provider.StdOut,
    "operator_io_filter" => Provider.Filter,
    "monitored_io_forward" => Provider.MonitoredIOForward,
    "operator_forward" => Provider.OperatorForward,
    # An `:on_message` provider that forwards a `{:flezha_request, key, payload,
    # reply_to, ref}` envelope to the SDK-side operator. Attach at operator
    # start with a hook keyed on the handler key, e.g.
    # `["on_message", {"key": "fetch_init_state"}]`.
    "operator_message_forward" => Provider.MessageForward
  }

  @doc "Every provider id to its module, first-party rows and installed ones."
  @spec all() :: %{binary() => module()}
  def all(), do: Map.merge(@first_party, :persistent_term.get(@installed_key, %{}))

  @doc "The rows the release itself provides."
  @spec first_party() :: %{binary() => module()}
  def first_party(), do: @first_party

  @doc "The module behind `provider_id`, or `:error`."
  @spec fetch(binary()) :: {:ok, module()} | :error
  def fetch(provider_id) when is_binary(provider_id), do: Map.fetch(all(), provider_id)

  @doc "Register a plugin package's provider as a row, refusing an id that already is one."
  @spec add(binary(), module()) :: :ok | {:error, {:provider_id_taken, binary(), module()}}
  def add(provider_id, module) when is_binary(provider_id) and is_atom(module) do
    case fetch(provider_id) do
      {:ok, holder} ->
        {:error, {:provider_id_taken, provider_id, holder}}

      :error ->
        installed = :persistent_term.get(@installed_key, %{})
        :persistent_term.put(@installed_key, Map.put(installed, provider_id, module))
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
  The worker families to supervise: one per provider row that declares a worker. Read by the application's supervision tree, so a provider that gains a
  worker gains its supervision from the same declaration.

  Sorted, because a supervision tree's child order is part of what the boot
  contract pins and a map's iteration order is not stable across compilations.
  """
  @spec worker_families() :: [module()]
  def worker_families() do
    for {_id, module} <- Enum.sort(all()),
        Code.ensure_loaded?(module),
        module.__provider__().worker,
        uniq: true,
        do: WorkerSup.family(module)
  end
end
