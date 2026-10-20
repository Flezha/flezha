defmodule Flezha.State.Ref.InputLog do
  @moduledoc "`{:input_log, relay_class}`: an instance's durable input log, read as a seq range."

  use Flezha.State.Ref

  alias Flezha.State.Paths

  def wal_path({:input_log, class}), do: Paths.input_log_path(class)
end
