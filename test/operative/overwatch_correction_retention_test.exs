defmodule FlezhaTest.Operative.OverwatchCorrectionRetentionTest do
  @moduledoc """
  A declared `correction_window` DERIVES the retention it needs rather than being
  refused against it. Retention is not a policy the author and the operator
  negotiate — a correction over a 60 s window MEANS holding 60 s of input log and
  60 s of WAL history, which is the definition of the declaration.

  But a one-line declaration can commit the controller to real disk, so a floor
  raised above its configured value is announced.
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.{ClassRegistry, Overwatch}
  alias Flezha.State.{InputLog, PersistenceResolver, TestHelpers}

  setup do
    base = TestHelpers.tmp_app_data_dir()
    ClassRegistry.ensure_table()

    prev_hold = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
    prev_age = Application.get_env(:flezha, :compact_after_age_ms, 0)

    on_exit(fn ->
      Application.put_env(:flezha, :input_log_hold_window_ms, prev_hold)
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(base)
    end)

    :ok
  end

  defp attach(event) do
    parent = self()
    ref = make_ref()
    handler = {__MODULE__, ref}

    :telemetry.attach(
      handler,
      event,
      fn _e, meas, meta, _ -> send(parent, {ref, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
    ref
  end

  test "a declared window raises both floors on default config, and says so" do
    class = "corrector-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    # Default config: no hold window at all, and an age floor shorter than the
    # window. This combination used to REFUSE the operative at start.
    Application.put_env(:flezha, :input_log_hold_window_ms, 0)
    Application.put_env(:flezha, :compact_after_age_ms, 60_000)
    :ok = ClassRegistry.put_correction_window(class, 300_000)

    assert :ok = Overwatch.announce_derived_retention(class, 300_000)

    assert InputLog.hold_window_ms(class) == 300_000
    assert PersistenceResolver.compact_after_age_ms(class) == 300_000

    assert_receive {^ref, %{configured: 0, derived: 300_000},
                    %{floor: :input_log_hold_window_ms, operative: ^class}}

    assert_receive {^ref, %{configured: 60_000, derived: 300_000},
                    %{floor: :compact_after_age_ms, operative: ^class}}
  end

  test "nothing is announced when the operator already configured enough" do
    class = "generous-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    Application.put_env(:flezha, :input_log_hold_window_ms, 600_000)
    Application.put_env(:flezha, :compact_after_age_ms, 600_000)
    :ok = ClassRegistry.put_correction_window(class, 60_000)

    assert :ok = Overwatch.announce_derived_retention(class, 60_000)

    assert InputLog.hold_window_ms(class) == 600_000
    assert PersistenceResolver.compact_after_age_ms(class) == 600_000

    refute_receive {^ref, _, _}, 50
  end

  test "an age floor of 0 is satisfied, not raised — the polarity trap" do
    class = "unbounded-hist-#{System.unique_integer([:positive])}"
    ref = attach([:flezha, :correction, :retention_derived])

    # 0 disables age compaction: WAL history is retained without an age bound,
    # which already satisfies any window. Raising it would ENABLE compaction and
    # make retention strictly worse.
    Application.put_env(:flezha, :input_log_hold_window_ms, 600_000)
    Application.put_env(:flezha, :compact_after_age_ms, 0)
    :ok = ClassRegistry.put_correction_window(class, 60_000)

    assert :ok = Overwatch.announce_derived_retention(class, 60_000)

    assert PersistenceResolver.compact_after_age_ms(class) == 0
    refute_receive {^ref, _, _}, 50
  end
end
