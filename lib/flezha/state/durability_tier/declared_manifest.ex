defmodule Flezha.State.DurabilityTier.DeclaredManifest do
  @moduledoc """
  `ProcessHub.Storage.RemoteManifest` adapter over the durability tier: the
  hub's declared list ships off-cluster as one more tier artifact class, through
  whichever backend the tier is already configured with — so the declared list
  gets the same storage options as every other durable artifact without a second
  remote-storage path beside the tier.

  The version-monotonic store contract ("never overwrite a higher version") maps
  onto the backend's `put_if/4` with the list version as the claim counter:
  `{:extend, version}` admits equal-or-lower stored versions, and a refusal
  means a newer copy is stored — reported as superseded `:ok`, exactly as the
  behaviour specifies. The key lives outside the tier GC's `blob/` scope.

  Configured by `HubManager` on the namespace hubs whenever the tier is enabled;
  upstream ships the blob asynchronously off the command path, so a backend
  outage never fails or delays a start/stop.
  """

  @behaviour ProcessHub.Storage.RemoteManifest

  alias Flezha.State.DurabilityTier.Backend

  @impl true
  def store(hub_id, version, blob, _opts) do
    with_backend(fn module, opts ->
      wrapped = :erlang.term_to_binary({version, blob})

      case module.put_if(key(hub_id), wrapped, {:extend, version}, opts) do
        :ok -> :ok
        # A higher version is already stored — this write is superseded.
        {:error, {:claim_held, _stored}} -> :ok
        {:error, _} = error -> error
      end
    end)
  end

  @impl true
  def fetch(hub_id, _opts) do
    with_backend(fn module, opts ->
      case module.get(key(hub_id), opts) do
        {:ok, wrapped} -> decode(wrapped)
        {:error, :not_found} -> :not_found
        {:error, _} = error -> error
      end
    end)
  end

  @impl true
  def info(_opts) do
    case Backend.resolve() do
      :none -> %{adapter: :durability_tier, backend: :none}
      {module, _opts} -> %{adapter: :durability_tier, backend: module}
    end
  end

  @impl true
  def validate_config(_opts) do
    case Backend.resolve() do
      :none -> {:error, :durability_tier_disabled}
      {_module, _opts} -> :ok
    end
  end

  defp with_backend(fun) do
    case Backend.resolve() do
      :none -> {:error, :durability_tier_disabled}
      {module, opts} -> fun.(module, opts)
    end
  end

  defp decode(wrapped) do
    case :erlang.binary_to_term(wrapped) do
      {version, blob} when is_integer(version) and version > 0 and is_binary(blob) ->
        {:ok, {version, blob}}

      _ ->
        {:error, :malformed_declared_manifest}
    end
  rescue
    ArgumentError -> {:error, :malformed_declared_manifest}
  end

  defp key(hub_id), do: "declared/" <> to_string(hub_id)
end
