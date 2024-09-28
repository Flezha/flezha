defmodule FlechaTest do
  use ExUnit.Case
  doctest Flecha

  @test_computation_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  setup_all do
    %{}
  end

  test "add computation" do
    computation = %Flecha.Computation{
      id: "add_compuation_test",
      factory_id: @test_computation_factory
    }

    assert Flecha.add_computation(computation) === {:ok, "computation added"}
  end

  test "validate computation length" do
    invalid_computation = %Flecha.Computation{
      id: "some_very_long_computation_id_that_should_fail",
      factory_id: @test_computation_factory
    }

    valid_computation = %Flecha.Computation{
      id: "computation_string_32_bytes_long",
      factory_id: @test_computation_factory
    }

    assert Flecha.add_computation(invalid_computation) ===
             {:error, "computation id exceeds 32 bytes"}

    assert Flecha.add_computation(valid_computation) === {:ok, "computation added"}
  end

  test "add new io generator" do
    computation = %Flecha.Computation{
      id: "add_io_generator_test",
      factory_id: @test_computation_factory
    }

    {:ok, "computation added"} = Flecha.add_computation(computation)

    assert Flecha.add_io(
             computation.id,
             "generator",
             %Flecha.IO.Generator{interval: 100},
             :input
           ) == {:ok, "IO created"}
  end

  test "add new io internal process" do
    computation = %Flecha.Computation{
      id: "add_io_internal_process_test",
      factory_id: @test_computation_factory
    }

    {:ok, "computation added"} = Flecha.add_computation(computation)

    assert Flecha.add_io(
             computation.id,
             "internal_process",
             %Flecha.IO.InternalProcess{output_pid: self()},
             :input
           ) == {:ok, "IO created"}
  end

  test "computation flow generation -> internal_process" do
    computation = %Flecha.Computation{
      id: "c_flow_test_generate_internal",
      factory_id: @test_compare_filter_factory,
      variables: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, "computation added"} = Flecha.add_computation(computation)

    assert Flecha.add_io(
             computation.id,
             "internal_process",
             %Flecha.IO.InternalProcess{output_pid: self()},
             :output
           ) == {:ok, "IO created"}

    assert Flecha.add_io(
             computation.id,
             "generator",
             %Flecha.IO.Generator{interval: 10000, startup_payload: true, message: "hello world"},
             :input
           ) == {:ok, "IO created"}

    receive do
      _msg ->
        assert true
    after
      1000 -> assert false
    end
  end
end
