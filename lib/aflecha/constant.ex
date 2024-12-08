defmodule Aflecha.Constant do
  defmacro __using__(_) do
    quote do
      @default_operator "default"
      @testnet_ws_uri "ws://localhost:4500"
      @computation_id_max_bytes 32

      @factory_do_nothing "do_nothing"
    end
  end
end
