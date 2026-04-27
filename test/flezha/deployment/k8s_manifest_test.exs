defmodule FlezhaTest.Deployment.K8sManifestTest do
  use ExUnit.Case, async: true

  @daemonset_path Path.expand("../../../kubernetes/flezha-daemonset.yaml", __DIR__)
  @configmap_path Path.expand("../../../kubernetes/flezha-configmap.yaml", __DIR__)

  # Hand-rolled string assertions deliberately — adding YamlElixir to the
  # production dep tree just for these manifest sanity checks isn't worth it.
  # The asserts below are tight enough to catch volume / env / keepalive
  # drift between the dev compose stack and the k8s manifests.

  describe "flezha-daemonset.yaml" do
    setup do
      assert File.exists?(@daemonset_path),
             "expected daemonset manifest at #{@daemonset_path}"

      %{contents: File.read!(@daemonset_path)}
    end

    test "declares a flezha-sockets volume", %{contents: contents} do
      assert contents =~ ~r/-\s+name:\s+flezha-sockets/,
             "missing `flezha-sockets` volume entry in daemonset"
    end

    test "mounts flezha-sockets at /flezha/sockets in the agent container",
         %{contents: contents} do
      mount_block_pattern =
        ~r/-\s+name:\s+flezha-sockets\s*\n\s+mountPath:\s+\/flezha\/sockets/

      assert contents =~ mount_block_pattern,
             "expected `- name: flezha-sockets` followed by `mountPath: /flezha/sockets`"
    end

    test "the agent container exposes FLEZHA_SOCKET_FILES_PATH=/flezha/sockets",
         %{contents: contents} do
      env_pair_pattern =
        ~r/-\s+name:\s+FLEZHA_SOCKET_FILES_PATH\s*\n\s+value:\s+"\/flezha\/sockets"/

      assert contents =~ env_pair_pattern,
             "expected env var FLEZHA_SOCKET_FILES_PATH set to \"/flezha/sockets\" in the agent container"
    end
  end

  describe "flezha-configmap.yaml" do
    setup do
      assert File.exists?(@configmap_path),
             "expected configmap manifest at #{@configmap_path}"

      %{contents: File.read!(@configmap_path)}
    end

    test "defines FLEZHA_OPERATOR_KEEPALIVE as a positive integer string in milliseconds",
         %{contents: contents} do
      keepalive_pattern = ~r/FLEZHA_OPERATOR_KEEPALIVE:\s+"(\d+)"/

      case Regex.run(keepalive_pattern, contents) do
        [_match, value_str] ->
          {value, ""} = Integer.parse(value_str)

          assert value > 0,
                 "FLEZHA_OPERATOR_KEEPALIVE must be a positive integer (got #{value_str})"

        nil ->
          flunk(
            "missing FLEZHA_OPERATOR_KEEPALIVE entry in configmap or value not quoted as a string"
          )
      end
    end
  end
end
