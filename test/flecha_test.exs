defmodule FlechaTest do
  use ExUnit.Case
  doctest Flecha

  @test_computation_template "do_nothing"

  setup_all do
    computation_engines =
      Application.get_env(:flecha_core, :computation_engines, %{})
      |> put_in(["default", :computation_templates, @test_computation_template], %{})

    Application.put_env(:flecha_core, :computation_engines, computation_engines)

    %{}
  end

  test "add computation" do
    computation = %Flecha.Computation{
      id: "add_compuation_test",
      template_id: @test_computation_template
    }

    assert Flecha.add_computation(computation) === {:ok, "computation added"}
  end

  # test "add new io generator" do
  #   computation = %Flecha.Computation{
  #     id: "add_io_generator_test",
  #     template: @test_computation_template
  #   }

  #   {:ok, "computation added"} = Flecha.add_computation(computation)

  #   assert Flecha.add_io(
  #            computation.id,
  #            "generator",
  #            %Flecha.IO.Generator{interval: 100},
  #            :input
  #          ) == {:ok, "IO created"}
  # end

  # test "add new io internal process" do
  #   computation = %Flecha.Computation{
  #     id: "add_io_internal_process_test",
  #     template: @test_computation_template
  #   }

  #   {:ok, "computation added"} = Flecha.add_computation(computation)

  #   assert Flecha.add_io(
  #            computation.id,
  #            "internal_process",
  #            %Flecha.IO.InternalProcess{output_pid: self()},
  #            :input
  #          ) == {:ok, "IO created"}
  # end

  # TODO:
  # test "computation flow generation -> internal_process" do
  #   computation = %Flecha.Computation{
  #     id: "computation_flow_test_generate_internal",
  #     template: @test_computation_template
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
  #            %Flecha.IO.Generator{interval: 10000, startup_payload: true},
  #            :input
  #          ) == {:ok, "IO created"}

  #   receive do
  #     _msg -> assert true
  #   after
  #     1000 -> assert false
  #   end
  # end
end
