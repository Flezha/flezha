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
      computation_id: "add_compuation_test",
      computation_name: @test_computation_template
    }

    assert Flecha.add_computation(computation) === {:ok, "computation added"}
  end

  # test "add new io generator" do
  #   {:ok, "pipeline created"} = Flecha.pipeline_new("new_pipeline_io_generator")

  #   assert Flecha.pipeline_io_new(
  #            "new_pipeline_io_generator",
  #            "generator",
  #            %Flecha.IO.Generator{interval: 100},
  #            :input
  #          ) == {:ok, "IO created"}
  # end

  # test "add new io internal process" do
  #   pipeline_id = "new_pipeline_internal_process"

  #   {:ok, "pipeline created"} = Flecha.pipeline_new(pipeline_id)

  #   assert Flecha.pipeline_io_new(
  #            pipeline_id,
  #            "internal_process",
  #            %Flecha.IO.InternalProcess{output_pid: self()},
  #            :input
  #          ) == {:ok, "IO created"}
  # end
end
