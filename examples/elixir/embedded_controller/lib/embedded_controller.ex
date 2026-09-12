defmodule EmbeddedController do
  @moduledoc """
  The host application. It starts no controller of its own: `:flezha` is a
  dependency, so the controller tree came up with this application.
  """

  @doc "Whether the controller in this node finished booting, and why not."
  def status, do: Flezha.Boot.Readiness.verdict()
end
