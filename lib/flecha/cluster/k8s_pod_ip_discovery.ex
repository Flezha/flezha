defmodule Flecha.Cluster.K8sPodIpDiscovery do
  alias Flecha.Cluster.DiscoveryProtocol

  @type t() :: %__MODULE__{}
  defstruct []

  defimpl DiscoveryProtocol, for: Flecha.Cluster.K8sPodIpDiscovery do
    @impl true
    def get_nodes(_struct) do
      # TODO:
    end

    @impl true
    def validate_settings(_struct) do
      with :ok <- validate_service_account_path(),
           :ok <- validate_master_node_host() do
        {:ok, __MODULE__}
      else
        {:error, reason} -> {:error, reason}
      end
    end

    defp validate_service_account_path() do
      service_account_path = Application.fetch_env!(:flecha_core, :k8s_service_account_path)

      cond do
        File.dir?(service_account_path) ->
          :ok

        true ->
          {:error, "Service account path does not exist: #{service_account_path}"}
      end
    end

    defp validate_master_node_host() do
      master_node_host = Application.fetch_env!(:flecha_core, :k8s_master_node_host)

      cond do
        String.valid?(master_node_host) && length(master_node_host) > 0 ->
          :ok

        true ->
          {:error, "Invalid or empty master node host: #{master_node_host}"}
      end
    end
  end

  # defp get_nodes(%State{topology: topology, config: config, meta: meta}) do
  #   service_account_path =
  #     Keyword.get(config, :kubernetes_service_account_path, @service_account_path)

  #   token = get_token(service_account_path)
  #   ssl_opts = get_ssl_opts(service_account_path)

  #   namespace = get_namespace(service_account_path, Keyword.get(config, :kubernetes_namespace))
  #   app_name = Keyword.fetch!(config, :kubernetes_node_basename)
  #   cluster_name = Keyword.get(config, :kubernetes_cluster_name, "cluster")
  #   service_name = Keyword.get(config, :kubernetes_service_name)
  #   selector = Keyword.fetch!(config, :kubernetes_selector)
  #   ip_lookup_mode = Keyword.get(config, :kubernetes_ip_lookup_mode, :endpoints)

  #   master_name = Keyword.get(config, :kubernetes_master, @kubernetes_master)
  #   cluster_domain = System.get_env("CLUSTER_DOMAIN", "#{cluster_name}.local")

  #   master =
  #     cond do
  #       String.ends_with?(master_name, cluster_domain) ->
  #         master_name

  #       String.ends_with?(master_name, ".") ->
  #         # The dot at the end is used to determine that the name is "final"
  #         master_name

  #       :else ->
  #         master_name <> "." <> cluster_domain
  #     end

  #   cond do
  #     app_name != nil and selector != nil ->
  #       selector = URI.encode(selector)

  #       path =
  #         case ip_lookup_mode do
  #           :endpoints -> "api/v1/namespaces/#{namespace}/endpoints?labelSelector=#{selector}"
  #           :pods -> "api/v1/namespaces/#{namespace}/pods?labelSelector=#{selector}"
  #         end

  #       headers = [{~c"authorization", ~c"Bearer #{token}"}]
  #       http_options = [ssl: ssl_opts, timeout: 15000]

  #       case :httpc.request(:get, {~c"https://#{master}/#{path}", headers}, http_options, []) do
  #         {:ok, {{_version, 200, _status}, _headers, body}} ->
  #           parse_response(ip_lookup_mode, Jason.decode!(body))
  #           |> Enum.map(fn node_info ->
  #             format_node(
  #               Keyword.get(config, :mode, :ip),
  #               node_info,
  #               app_name,
  #               cluster_name,
  #               service_name
  #             )
  #           end)

  #         {:ok, {{_version, 403, _status}, _headers, body}} ->
  #           %{"message" => msg} = Jason.decode!(body)
  #           warn(topology, "cannot query kubernetes (unauthorized): #{msg}")
  #           []

  #         {:ok, {{_version, code, status}, _headers, body}} ->
  #           warn(topology, "cannot query kubernetes (#{code} #{status}): #{inspect(body)}")
  #           meta

  #         {:error, reason} ->
  #           error(topology, "request to kubernetes failed!: #{inspect(reason)}")
  #           meta
  #       end

  #     app_name == nil ->
  #       warn(
  #         topology,
  #         "kubernetes strategy is selected, but :kubernetes_node_basename is not configured!"
  #       )

  #       []

  #     selector == nil ->
  #       warn(
  #         topology,
  #         "kubernetes strategy is selected, but :kubernetes_selector is not configured!"
  #       )

  #       []

  #     :else ->
  #       warn(topology, "kubernetes strategy is selected, but is not configured!")
  #       []
  #   end
  # end
end
