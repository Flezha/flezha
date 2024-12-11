defmodule AflechaTest do
  alias Aflecha.Computation.ComputeManager
  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha

  # @test_forward_orig_factory "forward_orig"
  # @test_store_input_factory "store_input"
  @test_computation_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  setup_all do
    %{}
  end

  test "websocket io" do
    computation = %Aflecha.Computation.Builder{
      id: "add_websocket_io_test",
      factory_id: @test_computation_factory
    }

    {:ok, _} = ComputeManager.start_computation(@default_operator, computation)

    res =
      Aflecha.add_io(
        @default_operator,
        computation.id,
        %Aflecha.IO.Handler.Websocket{uri: @testnet_ws_uri},
        :input,
        []
      )

    assert elem(res, 0) == :ok
    assert is_binary(elem(res, 1))
  end

  test "add new io internal process" do
    computation = %Aflecha.Computation.Builder{
      id: "add_io_internal_process_test",
      factory_id: @test_computation_factory
    }

    {:ok, _} = ComputeManager.start_computation(@default_operator, computation)

    res =
      Aflecha.add_io(
        @default_operator,
        computation.id,
        %Aflecha.IO.Handler.BeamProcess{output_pid: self()},
        :input,
        []
      )

    assert elem(res, 0) == :ok
    assert is_binary(elem(res, 1))
  end

  test "computation flow generation -> internal_process" do
    computation = %Aflecha.Computation.Builder{
      id: "c_flow_test_generate_internal",
      factory_id: @test_compare_filter_factory,
      variables: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, _} = ComputeManager.start_computation(@default_operator, computation)

    Aflecha.add_io(
      @default_operator,
      computation.id,
      %Aflecha.IO.Handler.BeamProcess{output_pid: self()},
      :output,
      []
    )

    # Aflecha.add_io(
    #   @default_operator,
    #   computation.id,
    #   %Aflecha.IO.Handler.StdIO{},
    #   :output,
    #   []
    # )

    {:ok, __id} =
      Aflecha.add_io(
        @default_operator,
        computation.id,
        %Aflecha.IO.Handler.Websocket{
          uri: @testnet_ws_uri,
          post_connect: %{
            payload: {
              :text,
              ~s(
                  {
                    "event_type": "subscribe",
                    "event_data": {
                      "default_message": "hello world",
                      "interval": 1
                    }
                  }
                )
            }
          }
        },
        :input,
        handling_type: :task
      )

    for _ <- 1..10 do
      receive do
        _msg ->
          assert true
      after
        10000 -> assert false
      end
    end
  end
end
