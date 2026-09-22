defmodule Flezha.Operative.Builder do
  @moduledoc """
  Constructor for operative specs handed to
  `Flezha.Service.OperativeManager.start_operative/2`.

  The `opts` keyword list is opaque to most callers; four keys are
  contractual — `:default_input_type` (the tag the Overwatch reads when
  metadata omits one), `:plugins` (subscribed at operative start),
  `:seed_input` (`{message, metadata}`, delivered as the instance's first
  input during its bind, on the entry channel `metadata["channel"]` names or
  the first declared one) and `:operative_class` (the on-disk persistence
  key, defaulting to `operative.id`).

  Invariant: `:kind` is the factory's, resolved at start from what its runners
  announced, and frozen for the operative's lifetime; a caller that sets one
  asserts it and is refused when the factory disagrees. See
  `Flezha.Operative.Kind` for the kind/capability model.
  """

  # What a caller may write. The kind is not among them: it is the factory's,
  # announced by its runners, and the start resolves it.
  @declared [:id, :factory_id, :init_values, :opts]

  @type t() :: %__MODULE__{
          id: String.t() | nil,
          factory_id: String.t(),
          kind: :stream | :workflow | nil,
          init_values: map(),
          opts: Keyword.t()
        }
  @enforce_keys [:factory_id]
  defstruct [:id, :factory_id, :kind, init_values: %{}, opts: []]

  @doc """
  Build a spec from named fields, so a caller that reached the controller over
  a wire never has to name this struct.

  `:opts` may be written as a map keyed by text, which is what a JSON document
  carries. A field a caller may not write (the kind among them) or a missing
  `:factory_id` is refused rather than guessed. A missing `:id` is not: the start authority generates one. A
  spec that is already built passes through.
  """
  @spec build(t() | keyword() | map()) :: {:ok, t()} | {:error, term()}
  def build(%__MODULE__{} = spec), do: {:ok, spec}

  def build(fields) when is_list(fields) or is_map(fields) do
    fields = Enum.to_list(fields)

    with :ok <- required(fields),
         {:ok, fields} <- declared(fields),
         {:ok, fields} <- resolve_opts(fields) do
      {:ok, struct!(__MODULE__, fields)}
    end
  end

  @doc """
  The `:seed_input` a first message makes: `text` on the entry channel
  `channel` names, or on the first declared one when `channel` is `nil`;
  `nil` when there is no message. A channel names a message's entry channel,
  so one without `text` is refused.
  """
  @spec seed_input(binary() | nil, binary() | nil) ::
          {:ok, {binary(), map()} | nil} | {:error, String.t()}
  def seed_input(nil, nil), do: {:ok, nil}
  def seed_input(text, nil) when is_binary(text), do: {:ok, {text, %{}}}

  def seed_input(text, channel) when is_binary(text) and is_binary(channel),
    do: {:ok, {text, %{"channel" => channel}}}

  def seed_input(nil, channel) when is_binary(channel),
    do: {:error, "--channel names the entry channel of --text"}

  def seed_input(_text, _channel), do: {:error, "the first message and its channel are text"}

  defp required(fields) do
    case Enum.reject([:factory_id], &Keyword.has_key?(fields, &1)) do
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
