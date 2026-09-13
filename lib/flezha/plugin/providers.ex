defmodule Flezha.Plugin.Providers do
  @moduledoc """
  The provider table: every provider id the system can enable, and the module
  behind it.

  A first-party provider is a row of `@first_party` and nothing else — no
  configuration entry, no supervisor veneer, no second list to keep in step. An
  installed package's provisions are added at run time through `add/2` and are
  resolved by `flz plugin enable`, `--plugins` decoding and `flz plugin list`
  exactly as a first-party row is; disabling a package drops its subscriptions
  and leaves its rows, because its modules stay loaded.

  Run-time rows live in a `:persistent_term` rather than the application
  environment: they are written once per install and read on every enable and
  every spec decode.
  """

  alias Flezha.Plugin.Provider
  alias Flezha.Plugin.Provider.WorkerSup

  @installed_key {__MODULE__, :installed}

  @first_party %{
    "operative_kafka_producer" => Provider.Kafka.Producer,
    "operative_postgres" => Provider.Postgres,
    "operative_websocket" => Provider.Websocket,
    "operative_http_poll" => Provider.HttpPoll,
    "operative_file_source" => Provider.File.Source,
    "operative_file_sink" => Provider.File.Sink,
    "operative_beam_msg" => Provider.BeamMsgForward,
    "operative_stdout" => Provider.StdOut,
    "operative_io_filter" => Provider.Filter,
    "monitored_io_forward" => Provider.MonitoredIOForward,
    "operative_forward" => Provider.OperativeForward,
    # An `:on_message` provider that forwards a `{:flezha_request, key, payload,
    # reply_to, ref}` envelope to the SDK-side operative. Attach at operative
    # start with a subscription keyed on the handler key, e.g.
    # `["on_message", {"key": "fetch_init_state"}]`.
    "operative_message_forward" => Provider.MessageForward
  }

  @doc "Every provider id to its module, first-party rows and installed ones."
  @spec all() :: %{binary() => module()}
  def all(), do: Map.merge(@first_party, :persistent_term.get(@installed_key, %{}))

  @doc "The module behind `provider_id`, or `:error`."
  @spec fetch(binary()) :: {:ok, module()} | :error
  def fetch(provider_id) when is_binary(provider_id), do: Map.fetch(all(), provider_id)

  @doc """
  Register an installed package's provision as a row. Idempotent, and the last
  registration for an id wins — an upgrade re-registers the same id against the
  freshly loaded module.
  """
  @spec add(binary(), module()) :: :ok
  def add(provider_id, module) when is_binary(provider_id) and is_atom(module) do
    installed = :persistent_term.get(@installed_key, %{})
    :persistent_term.put(@installed_key, Map.put(installed, provider_id, module))
  end

  @doc "Drop every run-time row. For tests that install and uninstall in one VM."
  @spec reset_installed() :: :ok
  def reset_installed(), do: :persistent_term.put(@installed_key, %{})

  @doc """
  The worker families to supervise: one per first-party provider that declares a
  worker. Read by the application's supervision tree, so a provider that gains a
  worker gains its supervision from the same declaration.

  Sorted, because a supervision tree's child order is part of what the boot
  contract pins and a map's iteration order is not stable across compilations.
  """
  @spec worker_families() :: [module()]
  def worker_families() do
    for {_id, module} <- Enum.sort(@first_party),
        Code.ensure_loaded?(module),
        module.__provider__().worker,
        uniq: true,
        do: WorkerSup.family(module)
  end
end
