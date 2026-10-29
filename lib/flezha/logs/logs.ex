defmodule Flezha.Logs do
  @moduledoc """
  The controller's durable logs: each a kind (`Flezha.Logs.Kinds`) of one log
  core, the same writer, rotation, retention, index, reader and codec for
  every kind. `Flezha.Service.Logs` reads them across the cluster.

  `append/2` is how a direct kind's producer writes a record; a bus-fed kind's
  sink writes its own.
  """

  alias Flezha.Logs.{Codec, Kinds, Writer}

  @doc """
  Append `record` to the direct kind `kind_id` and wait for it to be on disk:
  `:ok`, or `{:error, reason}` when the kind is off, its writer is closed, its
  queue is full (`:overloaded`) or the append outlived the kind's bound
  (`:timeout`). A refusal never raises into the producer.
  """
  @spec append(atom(), term()) :: :ok | {:error, term()}
  def append(kind_id, record) do
    kind = Kinds.fetch!(kind_id)

    with :ok <- Kinds.check(kind), do: Writer.append(kind, Codec.encode(kind, record))
  end
end
