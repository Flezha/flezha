defmodule Flezha.Identificator do
  @moduledoc """
  The controller's name registry and the naming scheme it hands out: `via`
  tuples for every per-runner process, the hub name for a namespace, and the
  unique-id mint. Wraps `Registry` rather than being a GenServer, so it is
  started with the registry's own `child_spec/1`.
  """

  alias Flezha.Process.Cspec

  use Flezha.Constant

  @typedoc "The kinds of identifier this tree bounds."
  @type kind ::
          :operator
          | :namespace
          | :plugin
          | :schedule_name
          | :token
          | :runner
          | :legacy_state_dir

  @typedoc "Why an identifier was refused."
  @type refusal ::
          {:too_short, kind(), pos_integer()}
          | {:too_long, kind(), pos_integer()}
          | {:malformed, kind()}

  # The separator and width of each field `mint/3` can append, so a minted id's
  # length is decided by its kind's row and never by the draw. `:hex` is six
  # strong random bytes. `:time` is a 45-bit millisecond time and 35 strong
  # random bits in lowercase base32: an id never recurs once its millisecond
  # has passed, and two nodes minting in the same millisecond still differ.
  @suffixes %{hex: {"_", 12}, time: {"-", 16}}

  # `<name>@<slot>` is one operator id, and the slot is an ISO-8601 instant,
  # so a schedule name may spend only what the slot leaves.
  @schedule_slot_bytes byte_size("@2026-01-01T00:00:00Z")

  # One row per kind of identifier: `{min, max, regex}` in bytes, `nil` for a
  # rule the kind does not carry, and the `suffix` shape `mint/3` appends when
  # it is not `:hex`. Every limit in this tree is a row here —
  # before, the same question was answered by an attribute in a fifth file, an
  # inline `byte_size(...) >= 8` and a truncation in Rust that the minting side
  # never read.
  #
  # `:runner` is bounded by what a data-socket name can hold. The transport
  # shortens a longer id into `<11 bytes>~<12 hex>`, so an unbounded mint gave
  # the controller one id and the socket another. The value is declared here,
  # at the foundation rank, and `Flezha.Transport.Native.stream_socket_id_max/0`
  # is pinned to it by test rather than read from it: this module names nothing
  # above its own layer.
  @kinds %{
    operator: %{min: 1, max: @operator_id_max_bytes, regex: nil, suffix: :time},
    runner: %{min: 1, max: @stream_socket_id_max_bytes, regex: nil},
    namespace: %{min: 1, max: 63, regex: ~r/\A[a-z][a-z0-9_-]{0,62}\z/},
    plugin: %{min: 1, max: 64, regex: ~r/^[a-z0-9][a-z0-9_]*$/},
    schedule_name: %{
      min: 1,
      max: @operator_id_max_bytes - @schedule_slot_bytes,
      regex: ~r/^[A-Za-z0-9][A-Za-z0-9_-]*$/
    },
    token: %{min: 8, max: 64, regex: ~r/^[0-9a-f]+$/},
    legacy_state_dir: %{min: 6, max: 255, regex: ~r/^[A-Za-z0-9_-]+$/}
  }

  @doc "Every declared kind."
  @spec kinds() :: [kind()]
  def kinds(), do: Map.keys(@kinds)

  @doc "The rule `kind` carries: `min` and `max` bytes, and its regex or `nil`."
  @spec rule(kind()) :: %{
          required(:min) => pos_integer(),
          required(:max) => pos_integer(),
          required(:regex) => Regex.t() | nil,
          optional(:suffix) => :time
        }
  def rule(kind), do: Map.fetch!(@kinds, kind)

  @doc "The byte ceiling `kind` allows. What a caller budgets a suffix against."
  @spec limit(kind()) :: pos_integer()
  def limit(kind), do: rule(kind).max

  @doc """
  Whether `value` may be an identifier of `kind`.

  A refusal names which rule was broken and the limit it broke, so each caller
  renders it in the words its own surface has always used.
  """
  @spec validate(kind(), term()) :: :ok | {:error, refusal()}
  def validate(kind, value) when is_binary(value) do
    %{min: min, max: max, regex: regex} = rule(kind)

    cond do
      byte_size(value) < min -> {:error, {:too_short, kind, min}}
      byte_size(value) > max -> {:error, {:too_long, kind, max}}
      regex != nil and not Regex.match?(regex, value) -> {:error, {:malformed, kind}}
      true -> :ok
    end
  end

  def validate(kind, _value), do: {:error, {:malformed, kind}}

  @doc """
  Mint an identifier of `kind` under `prefix`, bounded by the kind's own rule.

  The prefix is trimmed to whatever the rule leaves after the suffix, so
  what is minted always passes `validate/2` — a mint that could exceed its own
  limit is a name the rest of the system renames behind the minter's back.
  `reserve:` holds back bytes a caller appends afterwards, such as a partition
  member's `#p15`.

  A `:hex` id is drawn again when the registry already holds it. A `:time` id
  names an operator, which this registry never holds; the start that claims
  it is its only check.
  """
  @spec mint(kind(), binary(), keyword()) :: binary()
  def mint(kind, prefix, opts \\ []) when is_binary(prefix) do
    shape = Map.get(rule(kind), :suffix, :hex)
    {separator, width} = Map.fetch!(@suffixes, shape)
    budget = limit(kind) - width - byte_size(separator) - Keyword.get(opts, :reserve, 0)
    id = trim(prefix, budget) <> separator <> mint_suffix(shape)

    if shape == :hex and Registry.lookup(__MODULE__, id) != [],
      do: mint(kind, prefix, opts),
      else: id
  end

  # The prefix, cut to `budget` bytes, without the separator the cut may land
  # on: a namespace trimmed mid-word reads badly enough in a log line without
  # `colocation__<hex>` on top of it.
  defp trim(prefix, budget) when budget > 0 do
    prefix
    |> binary_part(0, min(byte_size(prefix), budget))
    |> whole_characters()
    |> String.replace(~r/[_-]+$/, "")
  end

  defp trim(_prefix, _budget), do: ""

  # A cut inside a multi-byte character would mint an id no JSON answer can carry.
  defp whole_characters(cut) do
    if String.valid?(cut),
      do: cut,
      else: whole_characters(binary_part(cut, 0, byte_size(cut) - 1))
  end

  defp mint_suffix(:hex) do
    6
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end

  defp mint_suffix(:time) do
    <<random::35, _::5>> = :crypto.strong_rand_bytes(5)
    Base.encode32(<<System.os_time(:millisecond)::45, random::35>>, case: :lower, padding: false)
  end

  def child_spec(opts) do
    Cspec.build(__MODULE__, {__MODULE__, :start_link, [opts]}, :worker_permanent, shutdown: 500)
  end

  def start_link(_) do
    Registry.start_link(
      keys: :unique,
      cells: 1,
      name: __MODULE__
    )
  end

  @doc """
  A fresh runner id under `namespace`, bounded by the `:runner` rule so the
  data socket the controller opens carries the id the controller reports.
  """
  @spec unique_id(binary()) :: binary()
  def unique_id(namespace), do: mint(:runner, namespace)

  def unit_sup(runner_id) do
    gen_via(runner_id, "wus")
  end

  def control_channel(runner_id) do
    gen_via(runner_id, "cc")
  end

  def commits(runner_id) do
    gen_via(runner_id, "cmt")
  end

  def hub(namespace) do
    gen_id(namespace, "hub") |> String.to_atom()
  end

  @doc """
  Inverse of `hub/1`. `nil` for anything that is not a hub id.

  Reads a binary as well as an atom, so a caller holding a hub id it read off
  disk answers the question without minting an atom for it.
  """
  @spec namespace_from_hub(term()) :: binary() | nil
  def namespace_from_hub(hub_id) when is_atom(hub_id),
    do: namespace_from_hub(Atom.to_string(hub_id))

  def namespace_from_hub(hub_id) when is_binary(hub_id),
    do: String.replace_suffix(hub_id, "_hub", "")

  def namespace_from_hub(_), do: nil

  @doc """
  Whether `id` has the shape `hub/1` produces. Sifts hub children
  out of a supervisor's child list, and hub directories out of a registry
  root's listing.
  """
  @spec hub_id?(term()) :: boolean()
  def hub_id?(id) when is_atom(id), do: hub_id?(Atom.to_string(id))
  def hub_id?(id) when is_binary(id), do: String.ends_with?(id, "_hub")
  def hub_id?(_), do: false

  def stream_sup_primary(runner_id) do
    gen_via(runner_id, "scsp")
  end

  def stream_sup_secondary(runner_id) do
    gen_via(runner_id, "scss")
  end

  def control_unit(runner_id) do
    gen_via(runner_id, "cu")
  end

  def stream_channel_primary(runner_id, count_nr) do
    gen_via(runner_id, "scp_" <> Integer.to_string(count_nr))
  end

  def stream_channel_secondary(runner_id, count_nr) do
    gen_via(runner_id, "scs_" <> Integer.to_string(count_nr))
  end

  def string_to_pid(pname) when is_binary(pname) do
    pname = String.to_existing_atom(pname)

    case Process.whereis(pname) do
      nil -> {:error, "Process #{pname} not found"}
      pid -> {:ok, pid}
    end
  catch
    # `String.to_existing_atom/1` RAISES for a name no atom matches, so a
    # one-argument `catch` (throws only) let the documented refusal be skipped.
    _kind, _reason -> {:error, "Invalid PID format"}
  end

  defp gen_via(prefix, suffix) do
    {:via, Registry, {__MODULE__, gen_id(prefix, suffix)}}
  end

  defp gen_id(runner_id, type_str) when is_atom(runner_id) do
    gen_id(Atom.to_string(runner_id), type_str)
  end

  defp gen_id(runner_id, type_str) do
    runner_id <> "_" <> type_str
  end
end
