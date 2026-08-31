defmodule Flezha.Plugin.Installer.Supervisor do
  @moduledoc """
  Supervises the installable-plugin subtree. Three children in strict order
  so later ones can depend on earlier state:

      Flezha.Plugin.Installer.Store    # DETS-backed catalog
      Flezha.Plugin.Installer.Loader   # in-VM module registry (reads Store)
      Flezha.Plugin.Installer          # public service GenServer (reads Loader)

  Boot-time config validation rejects the inconsistent combination
  `require_signature: true` + `trusted_keys: []` — refusing to start
  rather than silently accepting signatures against an empty trust root.
  """

  use Supervisor

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    case validate_config() do
      :ok ->
        children = [
          Flezha.Plugin.Installer.Store,
          Flezha.Plugin.Installer.Loader,
          Flezha.Plugin.Installer
        ]

        Supervisor.init(children, strategy: :rest_for_one)

      {:error, reason} ->
        {:stop, {:config_invalid, reason}}
    end
  end

  @doc """
  Reads `:flezha, :plugin_installer` keyword list and ensures the signature
  policy is internally consistent. Reason atom names the first failure so
  runners can fix it from the startup log.
  """
  @spec validate_config() :: :ok | {:error, atom()}
  def validate_config do
    cfg = Application.get_env(:flezha, :plugin_installer, [])
    require_signature = Keyword.get(cfg, :require_signature, false)
    trusted_keys = Keyword.get(cfg, :trusted_keys, [])

    if require_signature and trusted_keys == [], do: {:error, :trusted_keys_empty}, else: :ok
  end
end
