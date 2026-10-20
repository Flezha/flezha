defmodule FlezhaTest.Relay.ClassRegistryTest do
  @moduledoc """
  `ClassRegistry` resolves what a class declared, and whether its instance is
  asleep, for paths that carry only the class string: in-memory ETS → durable
  sidecar → default. A sidecar exists only for a value other than its default,
  and every value survives ETS eviction through it.
  """
  # async: false: writes Application env; a shared on-disk root; node-global tables.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Suspend
  alias Flezha.State.Paths

  setup do
    ClassRegistry.ensure_table()

    %{class: "cr_" <> Integer.to_string(System.unique_integer([:positive]))}
  end

  test "an unknown class defaults to :flow", %{class: class} do
    assert ClassRegistry.get(class) == :flow
  end

  test "a relay runs in flow or in steps, and nothing else" do
    assert ClassRegistry.known_run?(:flow)
    assert ClassRegistry.known_run?(:steps)
    refute ClassRegistry.known_run?(:bounded)
  end

  test "a class keeps a task journal exactly when it runs in steps", %{class: class} do
    refute ClassRegistry.journals?(class)
    :ok = ClassRegistry.put(class, :steps)
    assert ClassRegistry.journals?(class)
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.journals?(class), "the run survives eviction"
  end

  test "a class persists unless it declares otherwise, whichever way it runs", %{class: class} do
    assert ClassRegistry.persistent?(class)

    :ok = ClassRegistry.put(class, :steps)
    :ok = ClassRegistry.put_persistent(class, false)
    refute ClassRegistry.persistent?(class)
    assert File.read!(Path.join(Paths.class_dir(class), "persistence_off")) == "true"

    :ok = ClassRegistry.delete(class)
    refute ClassRegistry.persistent?(class), "the switch survives eviction"

    :ok = ClassRegistry.put_persistent(class, true)
    assert ClassRegistry.persistent?(class)
    refute File.exists?(Path.join(Paths.class_dir(class), "persistence_off"))
  end

  test "the sleep interval is what the class announced and survives eviction",
       %{class: class} do
    refute ClassRegistry.sleeps?(class)

    :ok = ClassRegistry.put_sleep_after(class, 21_600_000)
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.sleep_after_ms(class) == 21_600_000
    assert ClassRegistry.sleeps?(class)

    :ok = ClassRegistry.put_sleep_after(class, nil)
    refute ClassRegistry.sleeps?(class)
  end

  test "a park record survives a table wipe and is gone once cleared", %{class: class} do
    assert Suspend.sleep_record(class) == nil

    :ok = Suspend.put_sleep(class, %{wake_deadline_ms: 1_700_000})
    :ets.delete_all_objects(:flezha_relay_class_registry)

    assert Suspend.sleep_record(class) == %{wake_deadline_ms: 1_700_000}

    :ok = Suspend.put_sleep(class, %{wake_deadline_ms: nil})
    :ok = ClassRegistry.delete(class)
    assert Suspend.sleep_record(class) == %{wake_deadline_ms: nil}

    :ok = Suspend.put_sleep(class, nil)
    :ets.delete_all_objects(:flezha_relay_class_registry)
    assert Suspend.sleep_record(class) == nil
  end

  test "an :flow put writes no sidecar; a :steps put does", %{class: class} do
    cont = class <> "_c"
    :ok = ClassRegistry.put(cont, :flow)
    refute File.exists?(sidecar(cont))

    :ok = ClassRegistry.put(class, :steps)
    assert File.read!(sidecar(class)) == "steps"
  end

  test "get falls back to the durable sidecar after the ETS entry is evicted", %{class: class} do
    :ok = ClassRegistry.put(class, :steps)
    :ok = ClassRegistry.delete(class)

    # ETS entry gone, durable sidecar remains → still resolves :steps, so a
    # process-less retention sweep over retained state gets the right kind.
    assert ClassRegistry.get(class) == :steps
  end

  test "a garbage sidecar resolves to the default", %{class: class} do
    File.mkdir_p!(Paths.class_dir(class))
    File.write!(sidecar(class), "definitely_not_a_kind")

    assert ClassRegistry.get(class) == :flow
  end

  test "effectively_once? defaults to false for an unknown class", %{class: class} do
    refute ClassRegistry.effectively_once?(class)
  end

  test "a tier-on put resolves true and writes the durable sidecar", %{class: class} do
    :ok = ClassRegistry.put_effectively_once(class, true)

    assert ClassRegistry.effectively_once?(class)
    assert File.exists?(tier_sidecar(class))

    # Survives ETS eviction via the sidecar, like the kind.
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.effectively_once?(class)
  end

  test "a tier-off put clears the entry and removes the sidecar", %{class: class} do
    :ok = ClassRegistry.put_effectively_once(class, true)
    :ok = ClassRegistry.put_effectively_once(class, false)

    refute ClassRegistry.effectively_once?(class)
    refute File.exists?(tier_sidecar(class))

    # Stays off after eviction — no stale sidecar resurrects the tier.
    :ok = ClassRegistry.delete(class)
    refute ClassRegistry.effectively_once?(class)
  end

  test "a tier-on put leaves a tier-off class's on-disk layout untouched", %{class: class} do
    :ok = ClassRegistry.put_effectively_once(class, false)
    refute File.exists?(Paths.class_dir(class))
  end

  test "ack_checkpoint? defaults to false — per-input is the tier's granularity",
       %{class: class} do
    refute ClassRegistry.ack_checkpoint?(class)

    :ok = ClassRegistry.put_effectively_once(class, true)

    refute ClassRegistry.ack_checkpoint?(class),
           "declaring the tier must not imply checkpoint granularity"
  end

  test "a checkpoint-ack put resolves true and survives ETS eviction", %{class: class} do
    :ok = ClassRegistry.put_ack_checkpoint(class, true)

    assert ClassRegistry.ack_checkpoint?(class)
    assert File.read!(checkpoint_sidecar(class)) == "true"

    # The durable sidecar answers after the in-memory entry is evicted — the
    # process-less Retention sweep and a fresh bind both read it.
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.ack_checkpoint?(class)
  end

  test "a class that drops the declaration reverts to per-input", %{class: class} do
    :ok = ClassRegistry.put_ack_checkpoint(class, true)
    :ok = ClassRegistry.put_ack_checkpoint(class, false)

    refute ClassRegistry.ack_checkpoint?(class)
    refute File.exists?(checkpoint_sidecar(class))

    :ok = ClassRegistry.delete(class)
    refute ClassRegistry.ack_checkpoint?(class)
  end

  test "put_declarations persists every declaration, and a rebind clears what was dropped",
       %{class: class} do
    # The bind path writes the whole decoded map in one call.
    :ok =
      ClassRegistry.put_declarations(class, %{
        effectively_once: true,
        ack_checkpoint: true,
        compensable_tasks: [0, 2],
        leaf_tasks: [1, 2],
        correction_window_ms: 60_000,
        compact_after_bytes: 4_194_304,
        durable: :remote,
        persistent: false,
        sleep_after_ms: 2_000
      })

    assert ClassRegistry.effectively_once?(class)
    assert ClassRegistry.ack_checkpoint?(class)
    assert ClassRegistry.compensable_tasks(class) == [0, 2]
    assert ClassRegistry.leaf_tasks(class) == [1, 2]
    assert ClassRegistry.correction_window_ms(class) == 60_000
    assert ClassRegistry.compact_after_bytes(class) == 4_194_304
    assert ClassRegistry.remote?(class)
    refute ClassRegistry.persistent?(class)
    assert ClassRegistry.sleep_after_ms(class) == 2_000

    # A class that rebinds having dropped its declarations must not read the
    # previous bind's — every sidecar goes with the claim.
    :ok =
      ClassRegistry.put_declarations(class, %{
        effectively_once: false,
        ack_checkpoint: false,
        compensable_tasks: nil,
        leaf_tasks: nil,
        correction_window_ms: nil,
        compact_after_bytes: nil,
        durable: :local,
        persistent: true,
        sleep_after_ms: nil
      })

    refute ClassRegistry.effectively_once?(class)
    refute ClassRegistry.ack_checkpoint?(class)
    refute ClassRegistry.compensable_tasks(class)
    refute ClassRegistry.leaf_tasks(class)
    refute ClassRegistry.correction_window_ms(class)
    refute ClassRegistry.compact_after_bytes(class)
    refute ClassRegistry.remote?(class)
    assert ClassRegistry.persistent?(class)
    refute ClassRegistry.sleeps?(class)
  end

  test "the declarations decode from the metadata, and the run from what is established" do
    established = %{run: :steps, each: :per_input, on_failure: :keep_progress}

    {:ok, raw, _size} =
      Protox.encode(%RunnerCommands.AddRelayResponse{
        metadata: %{
          "durable" => "remote",
          "sleep_after_ms" => "2000",
          "max_duration_ms" => "60000"
        }
      })

    raw = IO.iodata_to_binary(raw)

    assert %{
             run: :steps,
             each: :per_input,
             on_failure: :keep_progress,
             durable: :remote,
             persistent: true,
             sleep_after_ms: 2_000,
             max_duration_ms: 60_000,
             effectively_once: false
           } = ClassRegistry.decode_declarations(raw, established)

    assert %{run: :flow, durable: :local, sleep_after_ms: nil, persistent: true} =
             ClassRegistry.decode_declarations(<<>>, %{run: nil, each: nil, on_failure: nil})
  end

  test "remote durability defaults false, survives eviction, and reverts when dropped",
       %{class: class} do
    refute ClassRegistry.remote?(class)

    :ok = ClassRegistry.put_remote(class, true)
    assert ClassRegistry.remote?(class)
    assert File.read!(remote_sidecar(class)) == "true"

    # The commit gate is read on an instance a previous controller may have
    # bound, so it must answer without the in-memory entry.
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.remote?(class)

    :ok = ClassRegistry.put_remote(class, false)
    refute ClassRegistry.remote?(class)
    refute File.exists?(remote_sidecar(class))
  end

  describe "correction window declaration" do
    test "round-trips through ETS and survives eviction via the sidecar" do
      class = "corrector-#{System.unique_integer([:positive])}"

      :ok = ClassRegistry.put_correction_window(class, 60_000)
      assert ClassRegistry.correction_window_ms(class) == 60_000

      # The retention sweeps that derive from this have no Overwatch to ask and
      # may run before the class is ever bound — so it must outlive the table.
      :ets.delete(:flezha_relay_class_registry, {class, :correction_window_ms})
      assert File.exists?(correction_sidecar(class))
      assert ClassRegistry.correction_window_ms(class) == 60_000
    end

    test "an undeclaring class reads nil and writes no sidecar" do
      class = "plain-#{System.unique_integer([:positive])}"

      :ok = ClassRegistry.put_correction_window(class, nil)
      assert ClassRegistry.correction_window_ms(class) == nil
      refute File.exists?(correction_sidecar(class))
    end

    test "a class that drops the declaration on rebind stops reading the old one" do
      class = "dropper-#{System.unique_integer([:positive])}"

      :ok = ClassRegistry.put_correction_window(class, 60_000)
      :ok = ClassRegistry.put_correction_window(class, nil)

      assert ClassRegistry.correction_window_ms(class) == nil
      refute File.exists?(correction_sidecar(class))
    end
  end

  describe "the established language and kind" do
    test "is scoped to the namespace, not shared across the cluster" do
      factory = "shared-#{System.unique_integer([:positive])}"

      # The test fleet gives the same reference factory ids to every SDK, so a
      # cluster-global record would let one namespace's Rust runner refuse
      # another namespace's Java one.
      :ok = ClassRegistry.put_established("ns_a", factory, :sdk_lang, 1)
      :ok = ClassRegistry.put_established("ns_b", factory, :sdk_lang, 2)

      assert ClassRegistry.established("ns_a", factory, :sdk_lang) == 1
      assert ClassRegistry.established("ns_b", factory, :sdk_lang) == 2
    end

    test "an unestablished factory reads nil rather than a default" do
      assert ClassRegistry.established(
               "ns",
               "never-#{System.unique_integer([:positive])}",
               :sdk_lang
             ) == nil
    end

    test "deleting it lets the factory re-establish, and only in that namespace" do
      factory = "recycled-#{System.unique_integer([:positive])}"

      :ok = ClassRegistry.put_established("ns_a", factory, :sdk_lang, 1)
      :ok = ClassRegistry.put_established("ns_b", factory, :sdk_lang, 1)
      :ok = ClassRegistry.delete_established("ns_a", factory)

      assert ClassRegistry.established("ns_a", factory, :sdk_lang) == nil
      assert ClassRegistry.established("ns_b", factory, :sdk_lang) == 1

      :ok = ClassRegistry.put_established("ns_a", factory, :sdk_lang, 3)
      assert ClassRegistry.established("ns_a", factory, :sdk_lang) == 3
    end

    test "the kind is established beside the language and forgotten with it" do
      factory = "kinded-#{System.unique_integer([:positive])}"

      :ok = ClassRegistry.put_established("ns", factory, :sdk_lang, 1)
      :ok = ClassRegistry.put_established("ns", factory, :run, :steps)

      assert ClassRegistry.established("ns", factory, :run) == :steps
      assert ClassRegistry.established("ns", factory, :sdk_lang) == 1

      :ok = ClassRegistry.delete_established("ns", factory)
      assert ClassRegistry.established("ns", factory, :run) == nil
      assert ClassRegistry.established("ns", factory, :sdk_lang) == nil
    end
  end

  describe "the ownership record" do
    test "round-trips the Overwatch and its owner", %{class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "identity-a")
      assert ClassRegistry.owner(class) == {:ok, self(), "identity-a"}
      assert ClassRegistry.overwatch_pid(class) == {:ok, self()}

      # A bind names its runner.
      :ok = ClassRegistry.put_overwatch(class, self(), "identity-b")
      assert ClassRegistry.owner(class) == {:ok, self(), "identity-b"}
    end

    test "is removed with the class", %{class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "identity-a")
      :ok = ClassRegistry.delete(class)

      assert ClassRegistry.owner(class) == :error
    end

    test "answers :error for a class never bound here", %{class: class} do
      assert ClassRegistry.owner(class) == :error
    end
  end

  defp sidecar(class), do: Path.join(Paths.class_dir(class), "run")
  defp tier_sidecar(class), do: Path.join(Paths.class_dir(class), "effectively_once")

  defp checkpoint_sidecar(class),
    do: Path.join(Paths.class_dir(class), "ack_checkpoint")

  defp remote_sidecar(class),
    do: Path.join(Paths.class_dir(class), "remote")

  defp correction_sidecar(class),
    do: Path.join(Paths.class_dir(class), "correction_window_ms")
end
