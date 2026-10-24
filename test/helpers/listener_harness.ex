defmodule FlezhaTest.Helper.ListenerHarness do
  @moduledoc """
  What the listener and part system tests read and attach: a relay bound to
  a runner, the listener rows it carries, and a file sink beside a listener
  with the lines it wrote.
  """

  import ExUnit.Assertions, only: [assert: 1, assert: 2]
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Config
  alias Flezha.Plugin
  alias Flezha.Relay.Builder
  alias Flezha.Service.{HubManager, RelayManager}
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @doc """
  Start `factory` as a relay under a fresh id with `plugins` bound, and answer
  the id once a runner holds it.
  """
  @spec start_bound!(String.t(), String.t(), [Plugin.t()]) :: String.t()
  def start_bound!(ns, factory, plugins \\ []) do
    id = "#{factory}_#{System.unique_integer([:positive])}"
    bindings = RelayFixture.bindings(ns, id, plugins)
    builder = %Builder{id: id, factory_id: factory, init_values: %{}, opts: [bindings: bindings]}

    assert {:ok, _} = RelayManager.start_relay(ns, builder)

    assert NodeTestHelper.wait_until(fn -> runner_holds?(ns, id) end, 30_000),
           "#{id} was never bound"

    id
  end

  @doc "Whether a runner holds the relay `id`."
  @spec runner_holds?(String.t(), String.t()) :: boolean()
  def runner_holds?(ns, id) do
    match?(
      {:ok, %{runner_id: runner}} when is_binary(runner),
      RelayManager.get_overwatch_state(ns, id)
    )
  end

  @doc """
  The listener plugins on `id`: on its live extension table and in its
  registry row, so a row left behind is found wherever the relay runs.
  """
  @spec listeners(String.t(), String.t()) :: [String.t()]
  def listeners(ns, id) do
    live =
      case RelayManager.get_overwatch_state(ns, id) do
        {:ok, %{extensions: table}} -> Map.keys(table.entries)
        _ -> []
      end

    declared =
      case HubManager.relay_bindings(ns, id) do
        {:ok, plugins} -> Enum.map(plugins, &elem(&1, 0))
        _ -> []
      end

    for "io_listener_" <> _ = plugin <- live ++ declared, do: plugin
  end

  @doc "A file sink on the output chain, writing to `path` under the file plugin root."
  @spec file_sink(String.t(), String.t()) :: Plugin.t()
  def file_sink(id, path) do
    %Plugin{
      id: id,
      provider: Flezha.Plugin.Provider.File.Sink,
      namespace: nil,
      relay_id: nil,
      opts: %{path: path},
      hooks: [{:on_output, []}]
    }
  end

  @doc """
  A path for `file_sink/2` that no other run has written: a file in a directory
  claimed under the file plugin root, removed when the test exits. The sink
  appends, so a name a previous run left behind would carry its lines.
  """
  @spec sink_path(String.t()) :: String.t()
  def sink_path(prefix) do
    root = Path.expand(Config.get(:file_plugin_root))
    File.mkdir_p!(root)
    dir = NodeTestHelper.claim_tmp_dir(prefix, root)
    on_exit(fn -> File.rm_rf(dir) end)
    dir |> Path.relative_to(root) |> Path.join("sink.ndjson")
  end

  @doc "The lines the file sink at `path` has written, `[]` before its first."
  @spec sink_lines(String.t()) :: [String.t()]
  def sink_lines(path) do
    {:ok, %{path: resolved}} = Flezha.Plugin.Provider.File.prepare(%{path: path, plugin_id: "x"})

    case File.read(resolved) do
      {:ok, text} -> String.split(text, "\n", trim: true)
      {:error, _} -> []
    end
  end
end
