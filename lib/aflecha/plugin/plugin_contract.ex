defmodule Aflecha.Plugin.PluginContract do
  @moduledoc """
  This module defines the behaviour that all plugins must implement.
  """

  @callback plugin_type() :: :worker_linked_process | :managed_process
  @callback load() :: :ok, {:error, term()}
end
