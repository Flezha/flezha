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

    test "mounts flezha-sockets at /flezha/sockets in the controller container",
         %{contents: contents} do
      mount_block_pattern =
        ~r/-\s+name:\s+flezha-sockets\s*\n\s+mountPath:\s+\/flezha\/sockets/

      assert contents =~ mount_block_pattern,
             "expected `- name: flezha-sockets` followed by `mountPath: /flezha/sockets`"
    end

    test "the controller container exposes FLEZHA_SOCKET_FILES_PATH=/flezha/sockets",
         %{contents: contents} do
      env_pair_pattern =
        ~r/-\s+name:\s+FLEZHA_SOCKET_FILES_PATH\s*\n\s+value:\s+"\/flezha\/sockets"/

      assert contents =~ env_pair_pattern,
             "expected env var FLEZHA_SOCKET_FILES_PATH set to \"/flezha/sockets\" in the controller container"
    end

    # Liveness and readiness are opposite questions in the same window: a
    # controller mid-replay must not be restarted (200 on /healthz) and must
    # not be routed to (503 on /readyz). One endpoint cannot answer both, so
    # the manifest is pinned against drifting the readiness probe back.
    test "readiness reads /readyz while liveness and startup read /healthz",
         %{contents: contents} do
      probe = fn name ->
        case Regex.run(~r/#{name}:\s*\n\s+httpGet:\s*\n\s+path:\s+(\S+)/, contents) do
          [_, path] -> path
          nil -> nil
        end
      end

      assert probe.("readinessProbe") == "/readyz"
      assert probe.("livenessProbe") == "/healthz"
      assert probe.("startupProbe") == "/healthz"
    end
  end

  describe "flezha-configmap.yaml" do
    setup do
      assert File.exists?(@configmap_path),
             "expected configmap manifest at #{@configmap_path}"

      %{contents: File.read!(@configmap_path)}
    end

    test "defines FLEZHA_RUNNER_REMOVAL_GRACE_MS as a positive integer string in milliseconds",
         %{contents: contents} do
      grace_pattern = ~r/FLEZHA_RUNNER_REMOVAL_GRACE_MS:\s+"(\d+)"/

      case Regex.run(grace_pattern, contents) do
        [_match, value_str] ->
          {value, ""} = Integer.parse(value_str)

          assert value > 0,
                 "FLEZHA_RUNNER_REMOVAL_GRACE_MS must be a positive integer (got #{value_str})"

        nil ->
          flunk(
            "missing FLEZHA_RUNNER_REMOVAL_GRACE_MS entry in configmap or value not quoted as a string"
          )
      end
    end
  end
end
