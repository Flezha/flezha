defmodule FlezhaTest.Helper.HookCopy do
  @moduledoc """
  What a test reads off a controller plugin's running copy: whether this
  controller's hook sink hears for it yet, whether it lags, and how many hook
  events wait in its queue.
  """

  import ExUnit.Assertions

  alias Flezha.Observability.HookSink
  alias Flezha.Observability.Registry
  alias Flezha.Plugin.Shared
  alias FlezhaTest.Helper.NodeTestHelper

  @doc "Wait until a copy listens here, which the sink learns of a moment after it joins."
  def await_listening do
    assert NodeTestHelper.wait_until(
             fn -> Registry.lookup_sink(HookSink.sink_name()) != :error end,
             2_000
           ),
           "no hook sink registered"
  end

  @doc "Assert `copy` reports `plugin_lagging` within two seconds and holds at most `bound` events."
  def assert_lagging(copy, bound) do
    assert NodeTestHelper.wait_until(
             fn -> match?({:unhealthy, {:plugin_lagging, _}}, Shared.health(copy, copy)) end,
             2_000
           ),
           "the copy never lagged"

    assert backlog(copy) <= bound
  end

  @doc "The hook events waiting in `copy`'s forwarder now."
  def backlog(copy), do: :sys.get_state(:sys.get_state(copy).hooks).backlog.frames
end
