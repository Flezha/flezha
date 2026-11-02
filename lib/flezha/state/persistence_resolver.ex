defmodule Flezha.State.PersistenceResolver do
  @moduledoc """
  Resolves per-Relay persistence configuration from two sources, in order of
  precedence: the per-instance metadata shipped with `AddRelay`, then the
  row's own default. Each setting has one configuration layer, its global row;
  time-travel retention alone also reads the `%{<RelayClass> => hours}` store
  `Steps.set_retention/3` writes.

  One resolver per key, each named for the key it resolves. The keys and their
  defaults are `docs/internal/state_persistence.md`'s configuration table,
  which is the one place they are enumerated; the module's job is the
  precedence, not the catalogue.

  A resolver returns the resolved value and never `nil`: absent metadata falls
  through to the default, so every caller gets a usable value without repeating
  the fallback.
  """

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.InputLog
  alias Flezha.State.Paths

  require Logger

  @spec retention_hours(binary(), map()) :: non_neg_integer()
  def retention_hours(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    case Map.fetch(Config.get(:time_travel_retention_hours), relay_class) do
      {:ok, hours} ->
        hours

      :error ->
        resolve(metadata, :time_travel_retention_hours, &non_neg_int?/1, fn ->
          Config.get(:default_time_travel_retention_hours)
        end)
    end
  end

  @doc """
  How many hours a finished instance of `relay_class` keeps its state
  (retained Cells and completion record) before the retention sweeper reclaims
  it; `0` keeps nothing. Its class's `keep_finished` hours, or for `true` the
  controller's window (`:default_relay_completed_retention_hours`, env
  `FLEZHA_STEPS_RETENTION_HOURS`). A class that declares nothing keeps for that
  window only when `:keep_finished_by_default` is set; a memory-only class
  keeps nothing.
  """
  @spec completed_retention_hours(binary()) :: non_neg_integer()
  def completed_retention_hours(relay_class) when is_binary(relay_class) do
    declared = ClassRegistry.keep_finished(relay_class)

    cond do
      is_integer(declared) -> declared
      not ClassRegistry.persistent?(relay_class) -> 0
      declared == :default or Config.get(:keep_finished_by_default) -> window()
      true -> 0
    end
  end

  @doc """
  When a kept completion record of `relay_class` stamped `finished_at` expires,
  in Unix seconds: after its declared hours, else the controller's window,
  because a record exists only for state that was kept.
  """
  @spec kept_until(binary(), integer()) :: integer()
  def kept_until(relay_class, finished_at) when is_binary(relay_class) do
    hours =
      case ClassRegistry.keep_finished(relay_class) do
        hours when is_integer(hours) -> hours
        _default_or_undeclared -> window()
      end

    finished_at + hours * 3600
  end

  defp window(), do: Config.get(:default_relay_completed_retention_hours)

  @doc """
  The per-Cell WAL size at which the Watchdog triggers a compaction, per
  relay class. Precedence: the instance-metadata argument → the SDK-declared
  value the class shipped in its `AddRelay` metadata (persisted in
  `ClassRegistry`, resolved here the way `compact_after_age_ms/2` resolves a
  declared correction window) → global `:default_compact_after_bytes` (8 MiB).

  The SDK declaration is consulted through `ClassRegistry` rather than the
  `metadata` argument, because the Watchdog — the only caller — resolves this
  with only the class in hand.
  """
  @spec compact_after_bytes(binary(), map()) :: pos_integer()
  def compact_after_bytes(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    resolve(metadata, :compact_after_bytes, &pos_int?/1, fn ->
      declared_compact_after_bytes(relay_class)
    end)
  end

  # SDK-declared threshold (durable in ClassRegistry) → global default.
  defp declared_compact_after_bytes(relay_class) do
    case ClassRegistry.compact_after_bytes(relay_class) do
      v when is_integer(v) and v > 0 ->
        v

      _ ->
        Config.get(:default_compact_after_bytes)
    end
  end

  @doc """
  Whether `relay_class` keeps its work `durable: remote`: its commit
  barriers reach the remote durability tier before they are acked
  (`durability-remote-tier`, RPO 0). The class's own setting and nothing else:
  a behaviour of a relay is declared in its class.
  """
  @spec remote?(binary()) :: boolean()
  def remote?(relay_class) when is_binary(relay_class),
    do: ClassRegistry.remote?(relay_class)

  @spec output_stall_timeout_ms(binary(), map()) :: non_neg_integer()
  def output_stall_timeout_ms(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    resolve(metadata, :output_stall_timeout_ms, &non_neg_int?/1, fn ->
      Durations.get(:output_stall_timeout_ms)
    end)
  end

  @spec credit_window_bytes(binary(), map()) :: pos_integer()
  def credit_window_bytes(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    metadata
    |> resolve(:credit_window_bytes, &pos_int?/1, &wal_high_watermark_bytes/0)
    |> cap_to_namespace_mailbox_quota(relay_class)
  end

  # Per-namespace mailbox-byte quota, folded into the same cascade every
  # buffered-byte window resolves through: while authz is enforced, no single
  # Relay's credit window may exceed its namespace's
  # `max_mailbox_bytes_total` (finish-platform-authn-authz §6.3).
  defp cap_to_namespace_mailbox_quota(window, relay_class) do
    if Flezha.AuthZ.enabled?() do
      namespace = Paths.namespace_of_class(relay_class)

      case NamespaceStore.get(namespace) do
        {:ok, %{quotas: %{max_mailbox_bytes_total: limit}}}
        when is_integer(limit) and limit > 0 ->
          min(window, limit)

        _ ->
          window
      end
    else
      window
    end
  rescue
    _ -> window
  catch
    _, _ -> window
  end

  @doc """
  The WAL soft watermark (`:wal_high_watermark_bytes`) — the one definition of
  "how many bytes is too many for one Cell-scale payload".

  It bounds the per-Cell WAL, the input log, a steps journal entry, the
  default credit window, and a live state read, so it lives here rather than as
  a literal at each of those.
  """
  @spec wal_high_watermark_bytes() :: pos_integer()
  def wal_high_watermark_bytes(),
    do: Config.get(:wal_high_watermark_bytes)

  @doc """
  WAL age at which the Watchdog triggers a compaction, per relay.

  Per-relay rather than global because it stopped being a tuning knob: a
  correction rewinds inside its declared window, and compaction truncates the
  WAL below each snapshot — so an age-floor shorter than the correction window
  destroys the very history the rewind needs a baseline inside. A declared
  correction window therefore RAISES this floor to at least itself.

  `0` disables age-triggered compaction entirely — WAL history is then retained
  without an age bound, which already satisfies any window, so `0` is preserved
  rather than raised. The polarity is the opposite of the input-log hold window's
  and conflating the two is the one way to get this wrong: raising `0` here would
  *enable* compaction and make retention worse.
  """
  @spec compact_after_age_ms(binary(), map()) :: non_neg_integer()
  def compact_after_age_ms(relay_class \\ nil, metadata \\ %{})

  def compact_after_age_ms(nil, _metadata), do: Durations.get(:compact_after_age_ms)

  def compact_after_age_ms(relay_class, metadata) when is_binary(relay_class) do
    case configured_compact_after_age_ms(relay_class, metadata) do
      0 -> 0
      n -> max(n, ClassRegistry.correction_window_ms(relay_class) || 0)
    end
  end

  @doc """
  The age floor BEFORE a declared correction window raises it — what the user
  actually configured. For announcing a derivation, never for compacting against.
  """
  @spec configured_compact_after_age_ms(binary(), map()) :: non_neg_integer()
  def configured_compact_after_age_ms(relay_class, metadata \\ %{})
      when is_binary(relay_class) do
    resolve(metadata, :compact_after_age_ms, &non_neg_int?/1, fn ->
      Durations.get(:compact_after_age_ms)
    end)
  end

  @doc """
  How many released bytes a per-instance record log accumulates before a
  prefix cut rewrites it (`:record_log_cut_after_bytes`). A cut is asked for at
  every iteration commit; the rewrite — a copy, an fsync and a rename — is paid
  once per this many bytes, so a loop's logs stay bounded without a rewrite
  per turn.
  """
  @spec record_log_cut_after_bytes() :: non_neg_integer()
  def record_log_cut_after_bytes(), do: Config.get(:record_log_cut_after_bytes)

  # The `AddRelay` instance metadata (string or atom key) when `valid?` takes
  # it, else the default: metadata comes from a runner, not the boot-checked
  # configuration, so it is still judged here.
  defp resolve(metadata, meta_key, valid?, default) do
    case Map.get(metadata, Atom.to_string(meta_key), Map.get(metadata, meta_key)) do
      nil -> default.()
      v -> if valid?.(v), do: v, else: default.()
    end
  end

  defp non_neg_int?(v), do: is_integer(v) and v >= 0
  defp pos_int?(v), do: is_integer(v) and v > 0

  @doc """
  Announce the retention a declared correction window derived for itself.

  Retention is not a prerequisite a user negotiates — a correction over a
  60 s window MEANS holding 60 s of input log and 60 s of WAL history, so the
  declaration derives both floors (`InputLog.hold_window_ms/1`,
  `PersistenceResolver.compact_after_age_ms/1`). But an SDK author's one line can
  commit the controller to real disk, so a floor RAISED above its configured value
  is logged and emitted once, at bind. Nothing is announced when nothing was
  raised.
  """
  @spec announce_derived_retention(binary(), pos_integer()) :: :ok
  def announce_derived_retention(relay_class, window_ms) do
    configured_hold = Durations.get(:input_log_hold_window_ms)
    derived_hold = InputLog.hold_window_ms(relay_class)

    if derived_hold > configured_hold do
      emit_retention_derived(relay_class, :input_log_hold_window_ms, configured_hold,
        derived: derived_hold,
        window_ms: window_ms
      )
    end

    # `0` means "no age-triggered compaction", which already retains everything the
    # window needs — the resolver satisfies it rather than raising it.
    configured_age = configured_compact_after_age_ms(relay_class)
    derived_age = compact_after_age_ms(relay_class)

    if derived_age > configured_age do
      emit_retention_derived(relay_class, :compact_after_age_ms, configured_age,
        derived: derived_age,
        window_ms: window_ms
      )
    end

    :ok
  end

  defp emit_retention_derived(relay_class, floor, configured, opts) do
    derived = Keyword.fetch!(opts, :derived)

    Logger.info(
      "[Overwatch] #{relay_class} declares a #{Keyword.fetch!(opts, :window_ms)}ms " <>
        "correction window: #{floor} raised #{configured} -> #{derived}"
    )

    :telemetry.execute(
      [:flezha, :correction, :retention_derived],
      %{count: 1, configured: configured, derived: derived},
      %{relay: relay_class, floor: floor}
    )
  end
end
