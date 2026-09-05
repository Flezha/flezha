defmodule Flezha.Config do
  @moduledoc """
  Application-config reads shared by every layer: which plugins are enabled
  and how many data channels a runner opens. No state, so it sits below the
  services that ask.
  """

  @doc """
  Every provider id to its module. The list is `Flezha.Plugin.Providers`, not
  configuration: an installed package adds a row at run time, which a compiled
  config map cannot receive.
  """
  defdelegate get_enabled_plugins(), to: Flezha.Plugin.Providers, as: :all

  def concurrent_channel_count() do
    default = System.schedulers_online()
    channel_count = Application.get_env(:flezha, :concurrent_channel_count, 0)

    if channel_count > 0 do
      channel_count
    else
      default
    end
  end
end
