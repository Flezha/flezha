defmodule FlezhaTest.Fixtures.OperativeFixture do
  @moduledoc """
  Builds, starts, stops and plugs an operative for a test: a `Builder` with the
  suite's default namespace, and the output-forward plugin most scenarios
  attach to watch what it emits.
  """

  alias Flezha.Identificator
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias ProcessHub.Constant.Hook

  use Flezha.Constant

  def get(id, factory_id) do
    %Flezha.Operative.Builder{
      id: id || "my_operative",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(operative, namespace \\ @default_namespace) do
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

    result = OperativeManager.start_operative(namespace, operative)

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

  def stop(operative_id, namespace \\ @default_namespace) do
    OperativeManager.stop_operative(namespace, operative_id)
  end

  def attach_output_msg(
        {:ok, %{operative_id: cid}} = start_res,
        namespace \\ @default_namespace
      ) do
    plugin_id = cid <> "_output_msg_handler"

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(
        plugin_id,
        %Flezha.Plugin{
          id: plugin_id,
          namespace: namespace,
          operative_id: cid,
          provider: Flezha.Plugin.Provider.BeamMsgForward,
          opts: %{recv_pid: self()},
          hooks: [{:on_output, []}]
        }
      )

    start_res
  end
end
