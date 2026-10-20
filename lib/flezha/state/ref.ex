defmodule Flezha.State.Ref do
  @moduledoc """
  A durable reference kind: what one shippable unit of state is on disk. A kind
  is the module named after it (`:effects_log` is `Ref.EffectsLog`) plus one row
  of the table below; `use Flezha.State.Ref` supplies the answers of a kind with
  nothing, and the kind overrides what it has.
  """

  @type t ::
          {:cell, {binary(), binary(), binary()}}
          | {:input_log, binary()}
          | {:effects_log, binary()}
          | {:cspec, {binary(), binary()}}
          | {:dlq, {binary(), binary()}}
          | {:control, {binary(), binary()}}

  # `kind => {tag, identity segments}`. The tag starts every storage key the
  # kind owns, so it is durable. The cspec and the control stores carry their
  # writer in the identity (per-node replicas); a Cell's state and parked
  # failures are arbitrated by the claim counter instead.
  @kinds %{
    cell: {"e", 3},
    input_log: {"i", 1},
    effects_log: {"f", 1},
    cspec: {"c", 2},
    dlq: {"d", 2},
    control: {"p", 2}
  }

  # Named, not aliased: the kind modules call the modules that call this one.
  @modules Map.new(@kinds, fn {kind, _} ->
             {kind, Module.concat(__MODULE__, Macro.camelize(Atom.to_string(kind)))}
           end)

  @doc "Local WAL path of a reference read as a seq range, `nil` for a packed one."
  @callback wal_path(t()) :: binary() | nil
  @doc "A packed reference's files holding bytes now, as `{root, names}`; `nil` for a WAL kind."
  @callback payload_files(t()) :: {binary(), [binary()]} | nil
  @doc "Every path a node-to-node transfer moves for `ref` (its WAL by default), present or not."
  @callback files(t()) :: [binary()]
  @doc "A basename off the wire resolved to one of `ref`'s paths, `nil` for any other name."
  @callback file_path(t(), binary()) :: binary() | nil
  @doc "Local snapshot files, oldest first."
  @callback snapshots(t()) :: [{non_neg_integer(), binary()}]
  @doc "Where a fetched snapshot at `seq` is written, `nil` for a kind without snapshots."
  @callback snapshot_path(t(), non_neg_integer()) :: binary() | nil
  @doc "The WAL-owner epoch a manifest write is fenced on, read without a writer; `0` unfenced."
  @callback epoch(t()) :: non_neg_integer()
  @doc "The saga compensation markers, which travel on the manifest rather than as blobs."
  @callback markers(t()) :: %{intent: boolean(), done: boolean()}
  @doc "The lineage (`%{id, trail}`), `nil` for none: only a Cell's writer mints one."
  @callback lineage(t()) :: %{id: binary(), trail: list()} | nil
  @doc "Whether `ref` has anything durable on disk right now."
  @callback has_local_state?(t()) :: boolean()

  defmacro __using__(_opts) do
    quote do
      @behaviour Flezha.State.Ref

      def wal_path(_ref), do: nil
      def payload_files(_ref), do: nil
      def files(ref), do: List.wrap(wal_path(ref))
      def file_path(ref, name), do: Enum.find(files(ref), &(Path.basename(&1) == name))
      def snapshots(_ref), do: []
      def snapshot_path(_ref, _seq), do: nil
      def epoch(_ref), do: 0
      def markers(_ref), do: %{intent: false, done: false}
      def lineage(_ref), do: nil

      def has_local_state?(ref), do: Flezha.State.Ref.holds?(payload_files(ref), wal_path(ref))

      defoverridable Flezha.State.Ref
    end
  end

  @doc "Every kind as `kind => {tag, identity segments}`."
  @spec kinds() :: %{atom() => {binary(), pos_integer()}}
  def kinds(), do: @kinds

  @doc "The module of `ref`'s kind."
  @spec module(t()) :: module()
  def module({kind, _identity}), do: Map.fetch!(@modules, kind)

  @doc "The state key `ref` lives under (its identity's first segment), the unit adopted."
  @spec state_key(t()) :: binary()
  def state_key({_kind, identity}) when is_tuple(identity), do: elem(identity, 0)
  def state_key({_kind, identity}), do: identity

  def wal_path(ref), do: module(ref).wal_path(ref)
  def payload_files(ref), do: module(ref).payload_files(ref)
  def files(ref), do: module(ref).files(ref)
  def file_path(ref, name), do: module(ref).file_path(ref, name)
  def snapshots(ref), do: module(ref).snapshots(ref)
  def snapshot_path(ref, seq), do: module(ref).snapshot_path(ref, seq)
  def epoch(ref), do: module(ref).epoch(ref)
  def markers(ref), do: module(ref).markers(ref)
  def lineage(ref), do: module(ref).lineage(ref)
  def has_local_state?(ref), do: module(ref).has_local_state?(ref)

  @doc "`{root, names}` keeping only the names under `root` that hold bytes."
  @spec present(binary(), [binary()]) :: {binary(), [binary()]}
  def present(root, names), do: {root, Enum.filter(names, &non_empty?(Path.join(root, &1)))}

  @doc "Whether a kind's packed files, else its WAL, hold bytes."
  @spec holds?({binary(), [binary()]} | nil, binary() | nil) :: boolean()
  def holds?({_root, names}, _wal_path), do: names != []
  def holds?(nil, wal_path), do: non_empty?(wal_path)

  defp non_empty?(nil), do: false
  defp non_empty?(path), do: match?({:ok, %{size: size}} when size > 0, File.stat(path))
end
