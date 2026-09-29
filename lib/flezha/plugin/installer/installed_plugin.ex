defmodule Flezha.Plugin.Installer.InstalledPlugin do
  @moduledoc """
  One row of `flz plugin list --installed`: a package id and what this
  controller holds of it.

    * `:loaded` - loaded at this boot
    * `:refused` - present at this boot and not loaded; `error` says why
    * `:pending` - an archive placed or replaced since this boot, loaded at the
      next restart; `loaded_version` is the version running now, if any
    * `:removed` - loaded at this boot, its archive since removed; unloaded at the
      next restart
  """

  @type status :: :loaded | :refused | :pending | :removed

  @type t :: %__MODULE__{
          id: binary(),
          version: binary() | nil,
          status: status(),
          loaded_version: binary() | nil,
          providers: [binary()],
          error: Flezha.Error.t() | nil
        }

  @enforce_keys [:id, :status]
  defstruct [:id, :version, :status, :loaded_version, :error, providers: []]
end
