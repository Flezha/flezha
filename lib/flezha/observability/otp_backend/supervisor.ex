defmodule Flezha.Observability.OtpBackend.Supervisor do
  @moduledoc """
  Supervisor for the built-in process-backed observability sources.

  Sits under `Flezha.Observability.Supervisor`, started AFTER the
  foundation `DefaultSinks` worker so the ring-buffer and PubSub sinks
  exist before the first poll emit. Each child is its own GenServer
  under `:one_for_one`, so a crash in one source (the most common case
  being the `:host` source on a non-Linux box) leaves the rest running.

  When the runtime flag `:observability, :otp_backend_enabled` is
  `false` the supervisor starts with no children at all and no source
  registers with the foundation Registry.
  """

  use Supervisor

  alias Flezha.Config

  alias Flezha.Observability.OtpBackend.{
    BeamVmSource,
    BoundedMailboxSource,
    HostSource,
    ProcessHubSource,
    TransportSource
  }

  @sources [
    BeamVmSource,
    HostSource,
    ProcessHubSource,
    TransportSource,
    BoundedMailboxSource
  ]

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children =
      if enabled?() do
        @sources
      else
        []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp enabled?() do
    Config.get(:observability_otp_backend_enabled)
  end

  @doc false
  @spec source_modules() :: [module()]
  def source_modules(), do: @sources
end
