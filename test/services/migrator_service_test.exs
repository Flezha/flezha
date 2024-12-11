defmodule AflechaTest.MigratorServiceTest do
  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha.Computation.MigratorService

  @computation_id "test_migrator"

  setup do
    computation = TestHelper.test_computation(@computation_id)

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
