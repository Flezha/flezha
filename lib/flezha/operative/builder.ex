defmodule Flezha.Operative.Builder do
  @moduledoc """
  Constructor for operative specs handed to
  `Flezha.Service.OperativeManager.start_operative/2`.

  The `opts` keyword list is opaque to most callers; four keys are
  contractual — `:default_input_type` (the tag the Overwatch reads when
  metadata omits one), `:plugins` (subscribed at operative start),
  `:seed_input` (delivered as the instance's first input during its bind) and
  `:operative_class` (the on-disk persistence key, defaulting to `operative.id`).

  Invariant: `:kind` is set at start and frozen for the operative's lifetime.
  See `Flezha.Operative.Kind` for the kind/capability model.
  """

  alias Flezha.Operative.Kind

  @declared [:id, :factory_id, :kind, :init_values, :opts]

  @type t() :: %__MODULE__{
          id: String.t(),
          factory_id: String.t(),
          kind: atom(),
          init_values: map(),
          opts: Keyword.t()
        }
  @enforce_keys [:id, :factory_id]
  defstruct [:id, :factory_id, kind: :stream, init_values: %{}, opts: []]

  @doc """
  Build a spec from named fields, so a caller that reached the controller over
  a wire never has to name this struct.

  `:kind` may be written as text, and `:opts` as a map keyed by text: both are
  what a JSON document carries. A field this struct does not declare, a kind
  this controller does not know, or a missing `:id` / `:factory_id` is refused
  rather than guessed. A spec that is already built passes through.
  """
  @spec build(t() | keyword() | map()) :: {:ok, t()} | {:error, term()}
  def build(%__MODULE__{} = spec), do: {:ok, spec}

  def build(fields) when is_list(fields) or is_map(fields) do
    fields = Enum.to_list(fields)

    with :ok <- required(fields),
         {:ok, fields} <- declared(fields),
         {:ok, fields} <- resolve_kind(fields),
         {:ok, fields} <- resolve_opts(fields) do
      {:ok, struct!(__MODULE__, fields)}
    end
  end

  defp required(fields) do
    case Enum.reject([:id, :factory_id], &Keyword.has_key?(fields, &1)) do
      [] -> :ok
      missing -> {:error, {:missing_fields, missing}}
    end
  end

  defp declared(fields) do
    case Enum.reject(Keyword.keys(fields), &(&1 in @declared)) do
      [] -> {:ok, fields}
      unknown -> {:error, {:unknown_fields, unknown}}
    end
  end

  # A kind written as text resolves only to a kind this controller already
  # knows, so no atom is minted from a wire value.
  defp resolve_kind(fields) do
    case Keyword.fetch(fields, :kind) do
      :error -> {:ok, fields}
      {:ok, kind} when is_atom(kind) -> {:ok, fields}
      {:ok, text} when is_binary(text) -> put_known_kind(fields, text)
      {:ok, other} -> {:error, {:unknown_kind, other}}
    end
  end

  defp put_known_kind(fields, text) do
    kind = String.to_existing_atom(text)

    if Kind.known?(kind),
      do: {:ok, Keyword.put(fields, :kind, kind)},
      else: {:error, {:unknown_kind, text}}
  rescue
    ArgumentError -> {:error, {:unknown_kind, text}}
  end

  # `opts` is opaque to this struct, so its vocabulary is not enumerated here.
  # A key written as text resolves only to an atom this node already holds,
  # which is what stops a wire value minting one.
  defp resolve_opts(fields) do
    case Keyword.fetch(fields, :opts) do
      :error -> {:ok, fields}
      {:ok, opts} when is_list(opts) -> {:ok, fields}
      {:ok, %{} = opts} -> keyed_opts(Map.to_list(opts), [], fields)
      {:ok, other} -> {:error, {:unknown_opts, other}}
    end
  end

  defp keyed_opts([], done, fields), do: {:ok, Keyword.put(fields, :opts, Enum.reverse(done))}

  defp keyed_opts([{key, value} | rest], done, fields) when is_binary(key) do
    keyed_opts(rest, [{String.to_existing_atom(key), value} | done], fields)
  rescue
    ArgumentError -> {:error, {:unknown_opts, key}}
  end

  defp keyed_opts([{key, value} | rest], done, fields) when is_atom(key),
    do: keyed_opts(rest, [{key, value} | done], fields)
end
