defmodule Aflecha.Constant do
  defmacro __using__(_) do
    quote do
      @default_operator "default"
      @testnet_ws_uri "ws://localhost:4500"
    end
  end
end
