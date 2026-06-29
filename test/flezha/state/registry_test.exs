defmodule Flezha.State.RegistryTest do
  use ExUnit.Case, async: false

  alias Flezha.State.Registry

  setup do
    prev_env = System.get_env("FLEZHA_STATE_DIR")
    prev_app = Application.fetch_env(:flezha, :state_root)
    Application.delete_env(:flezha, :state_root)

    on_exit(fn ->
      case prev_app do
        {:ok, v} -> Application.put_env(:flezha, :state_root, v)
        :error -> Application.delete_env(:flezha, :state_root)
      end

      case prev_env do
        nil -> System.delete_env("FLEZHA_STATE_DIR")
        v -> System.put_env("FLEZHA_STATE_DIR", v)
      end
    end)

    :ok
  end

  # fails when: the controller ignores the operator-configured FLEZHA_STATE_DIR
  # and silently writes/restores the per-Segment WAL under the default
  # app_data/state, so a deployment that mounts a dedicated state volume warm-
  # restarts from an empty tree (the prod /flezha/state vs app_data/state mismatch).
  test "state_root/0 honors FLEZHA_STATE_DIR when no explicit :state_root is set" do
    System.put_env("FLEZHA_STATE_DIR", "/tmp/flezha-state-test-root")
    assert Registry.state_root() == "/tmp/flezha-state-test-root"
  end

  test "explicit :state_root application config takes precedence over FLEZHA_STATE_DIR" do
    System.put_env("FLEZHA_STATE_DIR", "/tmp/env-root")
    Application.put_env(:flezha, :state_root, "/tmp/app-config-root")
    assert Registry.state_root() == "/tmp/app-config-root"
  end

  test "state_root/0 falls back to app_data/state when neither is configured" do
    System.delete_env("FLEZHA_STATE_DIR")
    assert Registry.state_root() == "app_data/state"
  end
end
