defmodule Flezha.State.DurabilityTier.Backend.S3 do
  @moduledoc """
  Remote durability backend over an S3-compatible object store, through the
  signed-request client `Flezha.Http.S3`.

  `put_if/4` is the client's guarded write with this contract's compare: the
  guard's counter rides the object, and a guard the stored counter does not
  admit reports `{:claim_held, stored}`.

  Options: `:endpoint`, `:bucket`, `:region`, `:access_key_id`,
  `:secret_access_key_env`, `:prefix` (`flezha`).
  """

  @behaviour Flezha.State.DurabilityTier.Backend

  alias Flezha.Http.S3
  alias Flezha.State.DurabilityTier.Backend

  @impl true
  def child_spec(_opts), do: :ignore

  # Signing and addressing reach for these three on the first request, so a
  # boot without them selects a store that can never answer.
  @impl true
  def required_opts(), do: [:endpoint, :bucket, :access_key_id]

  @impl true
  def put(key, blob, opts), do: S3.put(key, blob, [], opts)

  @impl true
  def get(key, opts), do: S3.get(key, opts)

  @impl true
  def delete(key, opts), do: S3.delete(key, opts)

  @impl true
  def list(prefix, opts), do: S3.list(prefix, opts)

  @impl true
  def put_if(key, blob, guard, opts) do
    case S3.put_if(key, blob, elem(guard, 1), &Backend.guard_passes?(guard, &1), opts) do
      {:error, {:refused, stored}} -> {:error, {:claim_held, stored}}
      result -> result
    end
  end
end
