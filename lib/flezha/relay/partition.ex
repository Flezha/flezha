defmodule Flezha.Partition do
  @moduledoc """
  Key groups and contiguous range assignment for partitioned relays.

  A partitioned relay runs as N ordinary instances (`<id>#p0`..`<id>#p{N-1}`);
  every key hashes into one of #{256} key groups and each member owns a
  contiguous range of them. This module is the controller's half of that
  contract: the hash, the range split, and the coverage check the CLI renders.

  The hash is `Flezha.Relay.KeyGroup.group_of/1`, the Elixir SDK's FNV-1a-128, so
  the range a member is assigned here and the keys its runner admits are
  computed by one function.

  Ranges are stored and shipped explicitly (member cspec opts + the
  `AddRelay` wire fields), never re-derived from `(partition, partitions)`:
  a rescale recomputes them and a later weighted assignment needs no format
  change.
  """

  alias Flezha.Relay.KeyGroup

  @key_groups KeyGroup.key_groups()

  @typedoc "An inclusive `{from, to}` key-group range, both ends in `0..255`."
  @type range() :: {non_neg_integer(), non_neg_integer()}

  @doc """
  Whether `range` owns `key` — the one definition of "this is mine", shared by
  the source filter, the forward router and the keyed state read.
  """
  @spec owns?(range(), binary()) :: boolean()
  def owns?({from, to}, key) when is_binary(key) do
    group = KeyGroup.group_of(key)
    group >= from and group <= to
  end

  @doc """
  The `n` contiguous key-group ranges of an even split, in partition order.

  `ranges(3)` is `[{0, 84}, {85, 169}, {170, 255}]` — no gap, no overlap.
  """
  @spec ranges(pos_integer()) :: [range()]
  def ranges(n) when is_integer(n) and n >= 1 and n <= @key_groups do
    for p <- 0..(n - 1), do: {div(p * @key_groups, n), div((p + 1) * @key_groups, n) - 1}
  end

  @doc """
  Index of the range owning `group`, or `nil` when no range covers it.
  """
  @spec owner([range()], non_neg_integer()) :: non_neg_integer() | nil
  def owner(ranges, group) do
    Enum.find_index(ranges, fn {from, to} -> group >= from and group <= to end)
  end

  @doc """
  The element of `entries` whose `:range` owns `key`, or `nil`. `entries` is
  anything carrying a key-group range — the group's member rows, a forward's
  per-member workers.
  """
  @spec owner_of([map()], binary()) :: map() | nil
  def owner_of(entries, key) do
    Enum.find(entries, &owns?(&1.range, key))
  end

  @member_separator "#p"

  @doc """
  The instance id of member `index` of the group named `group_id`.
  """
  @spec member_id(String.t(), non_neg_integer()) :: String.t()
  def member_id(group_id, index), do: group_id <> member_suffix(index)

  @doc """
  The `#pK` suffix a member appends to ids it owns — its instance id, and the
  plugin instances fanned out to it.
  """
  @spec member_suffix(non_neg_integer()) :: String.t()
  def member_suffix(index), do: @member_separator <> Integer.to_string(index)

  @doc """
  Split an id carrying a member suffix into `{base, index}`, or `:error`.

  The inverse of `member_id/2`, and the one place the `#pK` convention is
  parsed — instance ids and the plugin ids a group enable fans out share it.
  """
  @spec split_member_suffix(String.t()) :: {String.t(), non_neg_integer()} | :error
  def split_member_suffix(id) when is_binary(id) do
    case String.split(id, @member_separator) do
      [_only] ->
        :error

      parts ->
        {suffix, base_parts} = List.pop_at(parts, -1)

        case Integer.parse(suffix) do
          {index, ""} -> {Enum.join(base_parts, @member_separator), index}
          _ -> :error
        end
    end
  end

  @opt_keys [:group, :partition, :partitions, :key_group_from, :key_group_to]

  @doc "The builder-opt keys that carry partition identity."
  @spec opt_keys() :: [atom()]
  def opt_keys, do: @opt_keys

  @doc """
  The opts a member's `Flezha.Relay.Builder` carries: durable in its cspec
  start-args, read back by the group views, the wire, and a rescale.
  """
  @spec member_opts(String.t(), non_neg_integer(), pos_integer(), range()) :: keyword()
  def member_opts(group_id, index, count, {from, to}) do
    [
      group: group_id,
      partition: index,
      partitions: count,
      key_group_from: from,
      key_group_to: to
    ]
  end

  @doc """
  The provider-visible half of a member's identity, merged into every plugin
  opts map a group fans out. Integers, because the JSON plugin-spec path
  cannot carry tuples.
  """
  @spec provider_opts(%{index: non_neg_integer(), count: pos_integer(), range: range()}) :: map()
  def provider_opts(%{index: index, count: count, range: {from, to}}) do
    %{
      partition_index: index,
      partition_count: count,
      key_group_from: from,
      key_group_to: to
    }
  end

  @doc """
  The partition identity carried by a builder's `opts`, or `nil` when the
  relay is not a group member. Accepts a keyword list or a map, since a
  cspec start-arg round-trips as either.
  """
  @spec identity(keyword() | map() | nil) ::
          %{group: String.t(), index: non_neg_integer(), count: pos_integer(), range: range()}
          | nil
  def identity(nil), do: nil

  def identity(opts) do
    get = fn key -> if is_map(opts), do: Map.get(opts, key), else: Keyword.get(opts, key) end

    with group when is_binary(group) <- get.(:group),
         count when is_integer(count) and count >= 1 <- get.(:partitions),
         index when is_integer(index) <- get.(:partition),
         from when is_integer(from) <- get.(:key_group_from),
         to when is_integer(to) <- get.(:key_group_to) do
      %{group: group, index: index, count: count, range: {from, to}}
    else
      _ -> nil
    end
  end

  @doc """
  The partition groups `rows` describe, one report per group.

  A report carries the group, how many members it declares, its members ordered
  by index, and its key-group `coverage/1`. Rows with no partition are not part
  of any group and are left out. Recomputed from the rows on every call — there
  is no stored group record that could disagree with them.
  """
  @spec groups([map()]) :: [map()]
  def groups(rows) when is_list(rows) do
    rows
    |> Enum.filter(&(&1[:partition] != nil))
    |> Enum.group_by(& &1.partition.group)
    |> Enum.map(fn {group, group_rows} ->
      members = Enum.sort_by(group_rows, & &1.partition.index)

      %{
        group: group,
        declared: members |> Enum.map(& &1.partition.count) |> Enum.max(),
        members: members,
        coverage: coverage(Enum.map(members, &{&1.id, &1.partition.range}))
      }
    end)
    |> Enum.sort_by(& &1.group)
  end

  @doc """
  Check that `ranges` (a `{label, {from, to}}` list) covers every key group
  exactly once.

  Returns `:ok`, or `{:error, %{gaps: [range], overlaps: [{group, [label]}]}}`.
  Coverage has no stored record that could diverge — it is recomputed from the
  member rows on every read, which is what makes a missing member visible.
  """
  @spec coverage([{term(), range()}]) ::
          :ok | {:error, %{gaps: [range()], overlaps: [{non_neg_integer(), [term()]}]}}
  def coverage(labelled_ranges) do
    owners =
      Enum.reduce(labelled_ranges, %{}, fn {label, {from, to}}, acc ->
        Enum.reduce(from..to//1, acc, fn group, acc ->
          Map.update(acc, group, [label], &(&1 ++ [label]))
        end)
      end)

    gaps = 0..(@key_groups - 1) |> Enum.reject(&Map.has_key?(owners, &1)) |> contiguous()
    overlaps = for {group, labels} <- Enum.sort(owners), length(labels) > 1, do: {group, labels}

    if gaps == [] and overlaps == [] do
      :ok
    else
      {:error, %{gaps: gaps, overlaps: overlaps}}
    end
  end

  # Collapse a sorted group list into inclusive ranges so a report names
  # "groups 85-169" rather than 85 separate numbers.
  defp contiguous([]), do: []

  defp contiguous([first | rest]) do
    rest
    |> Enum.reduce([{first, first}], fn group, [{from, to} | acc] ->
      if group == to + 1, do: [{from, group} | acc], else: [{group, group}, {from, to} | acc]
    end)
    |> Enum.reverse()
  end
end
