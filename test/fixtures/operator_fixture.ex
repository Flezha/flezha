defmodule FlezhaTest.Fixtures.OperatorFixture do
  @moduledoc """
  Builds, starts, stops and plugs an operator for a test: a `Builder` with the
  suite's default namespace, and the output-forward plugin most scenarios
  attach to watch what it emits.
  """

  alias Flezha.Identificator
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias ProcessHub.Constant.Hook

  use Flezha.Constant

  def get(id, factory_id) do
    %Flezha.Operator.Builder{
      id: id || "my_operator",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(operator, namespace \\ @default_namespace) do
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

    result = OperatorManager.start_operator(namespace, operator)

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

  def stop(operator_id, namespace \\ @default_namespace) do
    OperatorManager.stop_operator(namespace, operator_id)
  end

  def attach_output_msg(
        {:ok, %{operator_id: cid}} = start_res,
        namespace \\ @default_namespace
      ) do
    plugin_id = cid <> "_output_msg_handler"

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(
        plugin_id,
        %Flezha.Plugin{
          id: plugin_id,
          namespace: namespace,
          operator_id: cid,
          provider: Flezha.Plugin.Provider.BeamMsgForward,
          opts: %{recv_pid: self()},
          hooks: [{:on_output, []}]
        }
      )

    start_res
  end
end
