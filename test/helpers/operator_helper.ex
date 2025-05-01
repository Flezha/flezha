defmodule AflechaTest.Helpers.OperatorHelper do
  def start_operator(group_id \\ "test_temp_group") do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --manifest-path=#{current_path}/test/apps/rust_operator/Cargo.toml --target-dir=./target -- " <>
        group_id

    Aflecha.Service.EventManager.add_handler(:operator_connected_event)

    Aflecha.Service.PortPilot.open_port(command)

    receive do
      {:operator_connected_event, {^group_id, operator_id}} -> operator_id
    after
      2000 -> raise "temporary test operator not connected"
    end
  end

  def stop_operator(group_id, operator_id) do
    Aflecha.Service.EventManager.add_handler(:operator_disconnected_event)
    Aflecha.Operator.FleetManager.disconnect_operator(group_id, operator_id)

    receive do
      {:operator_disconnected_event, {^group_id, ^operator_id}} -> :ok
    after
      2000 -> raise "temporary test operator not disconnected"
    end
  end
end
