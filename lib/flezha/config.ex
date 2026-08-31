defmodule Flezha.Config do
  @moduledoc """
  Application-config reads shared by every layer: which plugins are enabled,
  what each provider advertises about itself, and how many data channels a
  runner opens. No state, so it sits below the services that ask.
  """

  def get_enabled_plugins() do
    Application.get_env(:flezha, :enabled_plugins)
  end

  def get_provider_info() do
    get_enabled_plugins()
    |> Enum.map(fn {id, module} ->
      info =
        try do
          module.provider_info()
        rescue
          _ ->
            %{module: inspect(module), type: :unknown, required_fields: [], optional_fields: []}
        end

      Map.put(info, :id, id)
    end)
  end

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
