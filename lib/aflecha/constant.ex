defmodule Aflecha.Constant do
  defmacro __using__(_) do
    quote do
      @default_operator "default"
      @testnet_ws_uri "ws://localhost:4500"
      @worker_id_max_bytes 32

      @factory_do_nothing "do_nothing"
      @factory_increment_state "increment_state"
    end
  end
end
