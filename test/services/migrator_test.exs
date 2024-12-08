defmodule AflechaTest.Service.MigratorTest do
  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha.Computation.Service.Migrator

  @computation_id "test_migrator"
  @factory_id "increment_state"

  setup do
    computation =
      AflechaTest.Fixtures.Computation.get(
        @computation_id,
        @factory_id
      )
      |> AflechaTest.Fixtures.Computation.start()

    on_exit(fn ->
      AflechaTest.Fixtures.Computation.stop(computation.id)
    end)

    %{computation: computation}
  end

  test "serialize computation" do
    serialized_data =
      Aflecha.Computation.Service.Migrator.serialize_computation(
        @default_operator,
        @computation_id
      )

    assert is_list(serialized_data)
    assert length(serialized_data) > 0
  end

  test "deserialize computation" do
    serialized_data =
      Aflecha.Computation.Service.Migrator.serialize_computation(
        @default_operator,
        @computation_id
      )

    assert :ok ===
             Aflecha.Computation.Service.Migrator.deserialize_computation(
               @default_operator,
               @computation_id,
               serialized_data
             )
  end
end
