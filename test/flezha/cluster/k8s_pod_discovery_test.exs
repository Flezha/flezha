defmodule Flezha.Cluster.K8sPodDiscoveryTest do
  @moduledoc """
  Kubernetes discovery refuses to boot without the mounted token and CA, so
  the service-account token is never sent to an unverified API server.
  """

  use FlezhaTest.Case, fixtures: [:env, :tmp_dir], async: false

  alias Flezha.Cluster.DiscoveryProtocol
  alias Flezha.Cluster.K8sPodDiscovery
  alias Flezha.Cluster.Orchestrator

  # The values deploy/kubernetes/ sets, over the config defaults.
  @settings %K8sPodDiscovery{
    node_base_name: "flezha",
    namespace: "flezha",
    cluster_name: "cluster",
    service_name: "flezha",
    selector: "app=flezha",
    master_name: "kubernetes.default.svc",
    master_port: 443
  }

  setup %{tmp: dir} do
    File.write!(Path.join(dir, "token"), "secret-token\n")
    File.write!(Path.join(dir, "ca.crt"), "")
    Application.put_env(:flezha, :k8s_service_account_path, dir)
    Application.put_env(:flezha, :k8s_cluster_domain, "")
    :ok
  end

  test "accepts a service account with token and CA, and an empty cluster domain" do
    assert {:ok, _} = DiscoveryProtocol.validate_settings(@settings)
  end

  for file <- ["ca.crt", "token"] do
    test "refuses a service account without #{file}", %{tmp: dir} do
      path = Path.join(dir, unquote(file))
      File.rm!(path)

      assert DiscoveryProtocol.validate_settings(%K8sPodDiscovery{}) ==
               {:error, {:service_account_file_missing, path}}
    end
  end

  test "the controller refuses to boot when the CA is missing", %{tmp: dir} do
    File.rm!(Path.join(dir, "ca.crt"))
    Application.put_env(:flezha, :cluster_discovery, "k8s_pod_discovery")

    assert_raise ArgumentError, ~r/service_account_file_missing/, fn ->
      Orchestrator.init([])
    end
  end

  test "a nil namespace is accepted: it is read from the service account" do
    assert {:ok, _} = DiscoveryProtocol.validate_settings(%{@settings | namespace: nil})
  end

  for key <- [:node_base_name, :selector, :master_name, :namespace, :service_name, :cluster_name],
      value <- ["", "  "] do
    test "refuses #{key} #{inspect(value)}" do
      settings = Map.put(@settings, unquote(key), unquote(value))

      assert DiscoveryProtocol.validate_settings(settings) ==
               {:error, {:invalid_setting, unquote(key), unquote(value)}}
    end
  end

  test "refuses a missing selector" do
    assert DiscoveryProtocol.validate_settings(%{@settings | selector: nil}) ==
             {:error, {:invalid_setting, :selector, nil}}
  end

  for port <- [0, 65_536, "443", nil] do
    test "refuses master port #{inspect(port)}" do
      assert DiscoveryProtocol.validate_settings(%{@settings | master_port: unquote(port)}) ==
               {:error, {:invalid_setting, :master_port, unquote(port)}}
    end
  end

  test "the controller refuses to boot with the default empty API server name" do
    Application.put_env(:flezha, :cluster_discovery, "k8s_pod_discovery")
    Application.delete_env(:flezha, :k8s_master_name)

    assert_raise ArgumentError, ~r/:invalid_setting, :master_name, ""/, fn ->
      Orchestrator.init([])
    end
  end
end
