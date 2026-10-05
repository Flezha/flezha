defmodule Flezha.Constant do
  @moduledoc """
  Compile-time constants every layer shares, injected by `use Flezha.Constant`.
  A macro rather than a function so the values fold at compile time and can be
  used in guards and module attributes.
  """

  defmacro __using__(_) do
    quote do
      @default_namespace "default"
      @relay_id_max_bytes 32

      # What a data-socket name can hold in its runner-id field. The transport
      # shortens anything longer into a name the minting side never chose, so
      # the mint reads the bound instead of discovering it. `flz_transport`'s
      # `STREAM_SOCKET_ID_MAX` is the same number; the two are held equal by
      # `test/flezha/process/identificator_test.exs`, because the foundation
      # layer names nothing above itself.
      @stream_socket_id_max_bytes 24

      @factory_do_nothing "do_nothing"
      @factory_increment_state "increment_state"
    end
  end
end
