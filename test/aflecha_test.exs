defmodule AflechaTest do
  use ExUnit.Case
  doctest Aflecha

  @default_operator "default"

  @test_computation_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"
  @test_forward_orig_factory "forward_orig"
  @test_store_input_factory "store_input"

  @testnet_ws_uri "ws://localhost:4500"

  setup_all do
    %{}
  end

  # test "add computation" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "add_compuation_test",
  #     factory_id: @test_computation_factory
  #   }

  #   assert Aflecha.add_computation(@default_operator, computation) === {:ok, "computation added"}
  # end

  # test "no operator specified" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "no_operator_test",
  #     factory_id: @test_computation_factory
  #   }

  #   assert Aflecha.add_computation(computation) === {:ok, "computation added"}
  #   assert Aflecha.rem_computation(computation.id) === {:ok, "computation removed"}
  # end

  # test "remove computation" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "rem_compuation_test",
  #     factory_id: @test_computation_factory
  #   }

  #   assert Aflecha.add_computation(@default_operator, computation) === {:ok, "computation added"}

  #   assert Aflecha.rem_computation(@default_operator, computation.id) ===
  #            {:ok, "computation removed"}
  # end

  # test "validate computation length" do
  #   invalid_computation = %Aflecha.Computation.Builder{
  #     id: "some_very_long_computation_id_that_should_fail",
  #     factory_id: @test_computation_factory
  #   }

  #   valid_computation = %Aflecha.Computation.Builder{
  #     id: "computation_string_32_bytes_long",
  #     factory_id: @test_computation_factory
  #   }

  #   assert Aflecha.add_computation(@default_operator, invalid_computation) ===
  #            {:error, "computation id exceeds 32 bytes"}

  #   assert Aflecha.add_computation(@default_operator, valid_computation) ===
  #            {:ok, "computation added"}
  # end

  # test "add new io generator" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "add_io_generator_test",
  #     factory_id: @test_computation_factory
  #   }

  #   {:ok, "computation added"} = Aflecha.add_computation(@default_operator, computation)

  #   res =
  #     Aflecha.add_input(
  #       @default_operator,
  #       computation.id,
  #       %Aflecha.IO.Generator{interval: 100},
  #       []
  #     )

  #   assert elem(res, 0) == :ok
  #   assert is_binary(elem(res, 1))
  # end

  # test "add new io internal process" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "add_io_internal_process_test",
  #     factory_id: @test_computation_factory
  #   }

  #   {:ok, "computation added"} = Aflecha.add_computation(@default_operator, computation)

  #   res =
  #     Aflecha.add_input(
  #       @default_operator,
  #       computation.id,
  #       %Aflecha.IO.InternalProcess{output_pid: self()},
  #       []
  #     )

  #   assert elem(res, 0) == :ok
  #   assert is_binary(elem(res, 1))
  # end

  test "computation flow generation -> internal_process" do
    computation = %Aflecha.Computation.Builder{
      id: "c_flow_test_generate_internal",
      factory_id: @test_compare_filter_factory,
      variables: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, "computation added"} = Aflecha.add_computation(@default_operator, computation)

    Aflecha.add_output(
      @default_operator,
      computation.id,
      %Aflecha.IO.InternalProcess{output_pid: self()},
      []
    )

    {:ok, __id} =
      Aflecha.add_input(
        @default_operator,
        computation.id,
        %Aflecha.IO.Websocket{
          uri: @testnet_ws_uri,
          post_connect: %{
            payload: {
              :text,
              "{\"event\":\"subscribe\", \"default_message\": \"hello world\"}"
            }
          }
        },
        handling_type: :task
      )

    for _ <- 1..1 do
      receive do
        _msg ->
          assert true
      after
        1000 -> assert false
      end
    end
  end

  # test "computation flow generation -> internal_process" do
  #   computation = %Aflecha.Computation.Builder{
  #     id: "c_flow_test_generate_internal",
  #     factory_id: @test_forward_orig_factory
  #   }

  #   {:ok, "computation added"} = Aflecha.add_computation(@default_operator, computation)

  #   Aflecha.add_output(
  #     @default_operator,
  #     computation.id,
  #     %Aflecha.IO.InternalProcess{output_pid: self()},
  #     []
  #   )

  #   {:ok, __id} =
  #     Aflecha.add_input(
  #       @default_operator,
  #       computation.id,
  #       %Aflecha.IO.Websocket{
  #         uri: "wss://ws.kraken.com",
  #         post_connect: %{
  #           payload: {
  #             :text,
  #             "{\"event\":\"subscribe\", \"subscription\":{\"name\": \"trade\"}, \"pair\":[\"BTC/EUR\", \"ETH/EUR\"]}"
  #           }
  #         }
  #       },
  #       handling_type: :task
  #     )

  #   for _ <- 1..10 do
  #     receive do
  #       msg ->
  #         IO.inspect(msg)
  #     after
  #       10000 -> assert false
  #     end
  #   end

  #   assert true
  # end
end
