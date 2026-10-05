defmodule FlezhaTest.Fixtures.RelayFixture do
  @moduledoc """
  Builds, starts, stops and plugs a relay for a test: a `Builder` with the
  suite's default namespace, and the output-forward plugin most scenarios
  attach to watch what it emits.
  """

  alias Flezha.Identificator
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias ProcessHub.Constant.Hook

  use Flezha.Constant

  def get(id, factory_id) do
    %Flezha.Relay.Builder{
      id: id || "my_relay",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(relay, namespace \\ @default_namespace) do
    self = self()

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %ProcessHub.Service.HookManager{
          id: "child_registered",
          m: Process,
          f: :send,
          a: [self, :hook_reg_insert_recv, []]
        }
      ]
    )

    result = RelayManager.start_relay(namespace, relay)

    receive do
      :hook_reg_insert_recv -> :ok
    after
      2000 -> raise "Timeout waiting for process registration"
    end

    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      ["child_registered"]
    )

    result
  end

  def stop(relay_id, namespace \\ @default_namespace) do
    RelayManager.stop_relay(namespace, relay_id)
  end

  @doc """
  The bindings a relay starting in `namespace` carries for `plugins`: each
  plugin is enabled as `<id>_<relay_id>`, one plugin per relay as a
  test's sink is (disabled again when the test ends), and bound, as
  `flz relay start --plugins` does. For `opts: [bindings: ...]`.
  """
  def bindings(namespace, relay_id, plugins) do
    plugins =
      Enum.map(plugins, fn plugin ->
        %{
          plugin
          | id: "#{plugin.id}_#{relay_id}",
            namespace: namespace,
            relay_id: relay_id
        }
      end)

    Enum.each(plugins, fn plugin ->
      ExUnit.Callbacks.on_exit(fn -> PluginManager.disable_plugin(namespace, plugin.id) end)
    end)

    {:ok, bindings} = PluginManager.start_bindings(namespace, plugins)
    bindings
  end

  def attach_output_msg(
        {:ok, %{relay_id: cid}} = start_res,
        namespace \\ @default_namespace
      ) do
    plugin_id = cid <> "_output_msg_handler"

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(
        plugin_id,
        %Flezha.Plugin{
          id: plugin_id,
          namespace: namespace,
          relay_id: cid,
          provider: Flezha.Plugin.Provider.BeamMsgForward,
          opts: %{recv_pid: self()},
          hooks: [{:on_output, []}]
        }
      )

    start_res
  end
end
