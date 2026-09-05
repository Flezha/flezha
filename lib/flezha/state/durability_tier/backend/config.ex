defmodule Flezha.State.DurabilityTier.Backend.Config do
  @moduledoc """
  Which backend the configuration names.

  Separate from `Flezha.State.DurabilityTier.Backend` on purpose: the contract
  must not name its own implementations. A behaviour that lists them is a
  dependency cycle with every module that implements it, and this is the whole
  of the resolution — one function, three shipped names, and the escape hatch
  that keeps "adding a backend is a module, not a fork" true.

  It is also deliberately a leaf: it names the three backends and nothing else,
  so a caller reaching it can never be dragged into the tier's own graph
  through it.
  """

  alias Flezha.Config
  alias Flezha.State.DurabilityTier.Backend

  @doc """
  The configured backend as `{module, opts}`, or `:none` when the tier is off.

  Reads `:flezha, :durability_tier`. The shipped names are `:postgres`, `:s3`
  and `:local_path`; an explicit `{module, opts}` tuple selects any module implementing
  `Backend`, which is the extension point that keeps "adding a backend is
  a module, not a fork" true. Anything else is `:none` — a typo must leave the
  controller running on local durability, never crash it into a boot loop.
  """
  @spec resolve() :: {module(), Backend.opts()} | :none
  def resolve() do
    opts = Config.group({:durability_tier, :backend_opts})

    case Config.get(:durability_backend) do
      :postgres -> {Backend.Postgres, opts}
      :s3 -> {Backend.S3, opts}
      :local_path -> {Backend.LocalPath, opts}
      {module, opts} when is_atom(module) and is_list(opts) -> {module, opts}
      _ -> :none
    end
  end
end
