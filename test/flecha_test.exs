defmodule FlechaTest do
  use ExUnit.Case
  doctest Flecha

  @test_computation_template "do_nothing"
  @test_compare_filter_template "compare_filter"

  setup_all do
    # TODO: need to think of another way to make sure engine is running.
    # computation_engines =
    #   Application.get_env(:flecha_core, :computation_engines, %{})
    #   |> put_in(["default", :computation_templates, @test_computation_template], %{})
    #   |> put_in(["default", :computation_templates, @test_compare_filter_template], %{})

    # Application.put_env(:flecha_core, :computation_engines, computation_engines)

    Process.sleep(1000)

    %{}
  end

  test "add computation" do
    computation = %Flecha.Computation{
      id: "add_compuation_test",
      template_id: @test_computation_template
    }

    assert Flecha.add_computation(computation) === {:ok, "computation added"}
  end

  test "validate computation length" do
    invalid_computation = %Flecha.Computation{
      id: "some_very_long_computation_id_that_should_fail",
      template_id: @test_computation_template
    }

    valid_computation = %Flecha.Computation{
      id: "computation_string_32_bytes_long",
      template_id: @test_computation_template
    }

    assert Flecha.add_computation(invalid_computation) ===
             {:error, "computation id exceeds 32 bytes"}

    assert Flecha.add_computation(valid_computation) === {:ok, "computation added"}
  end

  test "add new io generator" do
    computation = %Flecha.Computation{
      id: "add_io_generator_test",
      template_id: @test_computation_template
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
      template_id: @test_computation_template
    }

    {:ok, "computation added"} = Flecha.add_computation(computation)

    assert Flecha.add_io(
             computation.id,
             "internal_process",
             %Flecha.IO.InternalProcess{output_pid: self()},
             :input
           ) == {:ok, "IO created"}
  end

  # test "computation flow generation -> internal_process" do
  #   computation = %Flecha.Computation{
  #     id: "c_flow_test_generate_internal",
  #     template_id: @test_compare_filter_template,
  #     variables: %{"comparable" => "hello world", "inverted" => false}
  #   }

  #   {:ok, "computation added"} = Flecha.add_computation(computation)

  #   assert Flecha.add_io(
  #            computation.id,
  #            "internal_process",
  #            %Flecha.IO.InternalProcess{output_pid: self()},
  #            :output
  #          ) == {:ok, "IO created"}

  #   assert Flecha.add_io(
  #            computation.id,
  #            "generator",
  #            %Flecha.IO.Generator{interval: 10000, startup_payload: true, message: "hello world"},
  #            :input
  #          ) == {:ok, "IO created"}

  #   receive do
  #     _msg ->
  #       assert true
  #   after
  #     1000 -> assert false
  #   end
  # end
end
