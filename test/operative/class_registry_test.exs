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

  defp sidecar(class), do: Path.join([Registry.state_root(), class, "kind"])
end
