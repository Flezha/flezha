defmodule FlezhaTest.Deployment.K8sManifestTest do
  use ExUnit.Case, async: true

  @daemonset_path Path.expand("../../../deploy/kubernetes/flezha-daemonset.yaml", __DIR__)
  @configmap_path Path.expand("../../../deploy/kubernetes/flezha-configmap.yaml", __DIR__)
  @policy_path Path.expand("../../../deploy/kubernetes/flezha-networkpolicy.yaml", __DIR__)

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

    # A key the controller wrote 0600 must survive the init container's next
    # run: ownership is set, and a mode is only ever closed, on the top dirs.
    test "the init container sets ownership and widens no mode", %{contents: contents} do
      assert contents =~ "chown -R 1000:1000"
      modes = Regex.scan(~r/chmod\s+(-\S+|[0-7]{3,4})\b/, contents, capture: :all_but_first)
      assert modes != [] and Enum.all?(modes, &(&1 == ["700"])), inspect(modes)
    end

    test "pins FLEZHA_DIST_PORT and declares it beside EPMD", %{contents: contents} do
      port = dist_port(contents)
      assert contents =~ ~r/-\s+name:\s+dist\s*\n\s+containerPort:\s+#{port}\b/
      assert contents =~ ~r/-\s+name:\s+epmd\s*\n\s+containerPort:\s+4369\b/
    end
  end

  describe "flezha-networkpolicy.yaml" do
    setup do
      policy = File.read!(@policy_path)
      [_head, ingress] = String.split(policy, ~r/^  ingress:\n/m)
      rules = ingress |> String.split(~r/^    - /m, trim: true)
      %{policy: policy, rules: rules, dist_port: dist_port(File.read!(@daemonset_path))}
    end

    test "selects the controller pods", %{policy: policy} do
      assert policy =~ ~r/podSelector:\s*\n\s+matchLabels:\s*\n\s+app: flezha\n\s+policyTypes/
    end

    test "admits EPMD and the distribution port from controllers and the console alone",
         %{rules: rules, dist_port: dist_port} do
      for port <- [4369, dist_port] do
        assert [rule] = Enum.filter(rules, &(port in ports(&1))), "port #{port}"

        assert Regex.scan(~r/app: (\S+)/, rule, capture: :all_but_first) == [
                 ["flezha"],
                 ["console"]
               ]
      end
    end

    test "admits the health port from anywhere", %{rules: rules} do
      assert [rule] = Enum.filter(rules, &(9569 in ports(&1)))
      refute rule =~ "from:"
    end

    defp ports(rule),
      do:
        for(
          [p] <- Regex.scan(~r/port: (\d+)/, rule, capture: :all_but_first),
          do: String.to_integer(p)
        )
  end

  defp dist_port(daemonset) do
    [_, port] = Regex.run(~r/-\s+name:\s+FLEZHA_DIST_PORT\s*\n\s+value:\s+"(\d+)"/, daemonset)
    String.to_integer(port)
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
