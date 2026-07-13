defmodule FlezhaTest.Operative.ClassRegistryTest do
  @moduledoc """
  §1.2: `ClassRegistry` resolves `operative_class → kind` for the State
  subsystem — in-memory ETS → durable sidecar → default `:unbounded` — so the
  unbounded-only capabilities can be gated per class. An unknown class stays
  fully unbounded (gating is only ever subtractive); a `:bounded` class reads
  every unbounded-only flag false and its kind survives ETS eviction via the
  sidecar.
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{Registry, TestHelpers}

  setup do
    base = TestHelpers.tmp_app_data_dir()
    ClassRegistry.ensure_table()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{class: "cr_" <> Integer.to_string(System.unique_integer([:positive]))}
  end

  test "an unknown class defaults to :unbounded and reads every flag true", %{class: class} do
    assert ClassRegistry.get(class) == :unbounded
    assert ClassRegistry.capable?(class, :time_travel?)
    assert ClassRegistry.capable?(class, :dynamic_segments?)
    assert ClassRegistry.capable?(class, :output_stall_watch?)
  end

  test "a :bounded class resolves and reads every unbounded-only flag false", %{class: class} do
    :ok = ClassRegistry.put(class, :bounded)

    assert ClassRegistry.get(class) == :bounded
    refute ClassRegistry.capable?(class, :time_travel?)
    refute ClassRegistry.capable?(class, :dynamic_segments?)
    refute ClassRegistry.capable?(class, :output_stall_watch?)
  end

  test "an :unbounded put writes no sidecar; a :bounded put does", %{class: class} do
    cont = class <> "_c"
    :ok = ClassRegistry.put(cont, :unbounded)
    refute File.exists?(sidecar(cont))

    :ok = ClassRegistry.put(class, :bounded)
    assert File.read!(sidecar(class)) == "bounded"
  end

  test "get falls back to the durable sidecar after the ETS entry is evicted", %{class: class} do
    :ok = ClassRegistry.put(class, :bounded)
    :ok = ClassRegistry.delete(class)

    # ETS entry gone, durable sidecar remains → still resolves :bounded, so a
    # process-less retention sweep over retained state gets the right kind.
    assert ClassRegistry.get(class) == :bounded
    refute ClassRegistry.capable?(class, :time_travel?)
  end

  test "a garbage sidecar resolves to the default", %{class: class} do
    File.mkdir_p!(Path.join(Registry.state_root(), class))
    File.write!(sidecar(class), "definitely_not_a_kind")

    assert ClassRegistry.get(class) == :unbounded
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
    refute File.exists?(Path.join(Registry.state_root(), class))
  end

  test "checkpoint_ack? defaults to false — per-input is the tier's granularity",
       %{class: class} do
    refute ClassRegistry.checkpoint_ack?(class)

    :ok = ClassRegistry.put_effectively_once(class, true)

    refute ClassRegistry.checkpoint_ack?(class),
           "declaring the tier must not imply checkpoint granularity"
  end

  test "a checkpoint-ack put resolves true and survives ETS eviction", %{class: class} do
    :ok = ClassRegistry.put_checkpoint_ack(class, true)

    assert ClassRegistry.checkpoint_ack?(class)
    assert File.read!(checkpoint_sidecar(class)) == "true"

    # The durable sidecar answers after the in-memory entry is evicted — the
    # process-less Retention sweep and a fresh bind both read it.
    :ok = ClassRegistry.delete(class)
    assert ClassRegistry.checkpoint_ack?(class)
  end

  test "a class that drops the declaration reverts to per-input", %{class: class} do
    :ok = ClassRegistry.put_checkpoint_ack(class, true)
    :ok = ClassRegistry.put_checkpoint_ack(class, false)

    refute ClassRegistry.checkpoint_ack?(class)
    refute File.exists?(checkpoint_sidecar(class))

    :ok = ClassRegistry.delete(class)
    refute ClassRegistry.checkpoint_ack?(class)
  end

  test "put_declarations persists every declaration, and a rebind clears what was dropped",
       %{class: class} do
    # The bind path writes the whole decoded map in one call.
    :ok =
      ClassRegistry.put_declarations(class, %{
        effectively_once: true,
        checkpoint_ack: true,
        compensable_ops: [0, 2],
        leaf_ops: [1, 2]
      })

    assert ClassRegistry.effectively_once?(class)
    assert ClassRegistry.checkpoint_ack?(class)
    assert ClassRegistry.compensable_ops(class) == [0, 2]
    assert ClassRegistry.leaf_ops(class) == [1, 2]

    # A class that rebinds having dropped its declarations must not read the
    # previous bind's — every sidecar goes with the claim.
    :ok =
      ClassRegistry.put_declarations(class, %{
        effectively_once: false,
        checkpoint_ack: false,
        compensable_ops: nil,
        leaf_ops: nil
      })

    refute ClassRegistry.effectively_once?(class)
    refute ClassRegistry.checkpoint_ack?(class)
    refute ClassRegistry.compensable_ops(class)
    refute ClassRegistry.leaf_ops(class)
  end

  defp sidecar(class), do: Path.join([Registry.state_root(), class, "kind"])
  defp tier_sidecar(class), do: Path.join([Registry.state_root(), class, "effectively_once"])

  defp checkpoint_sidecar(class),
    do: Path.join([Registry.state_root(), class, "checkpoint_ack"])
end
