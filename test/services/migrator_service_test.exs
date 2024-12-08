defmodule AflechaTest.MigratorServiceTest do
  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha.Computation.MigratorService

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
      Aflecha.Computation.MigratorService.serialize_computation(
        @default_operator,
        @computation_id
      )

    assert is_list(serialized_data)
    assert length(serialized_data) > 0
  end

  test "deserialize computation" do
    serialized_data =
      Aflecha.Computation.MigratorService.serialize_computation(
        @default_operator,
        @computation_id
      )

    assert :ok ===
             Aflecha.Computation.MigratorService.deserialize_computation(
               @default_operator,
               @computation_id,
               serialized_data
             )
  end
end
