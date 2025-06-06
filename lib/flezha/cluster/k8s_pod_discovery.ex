defmodule Flezha.Cluster.K8sPodDiscovery do
  alias Flezha.Cluster.DiscoveryProtocol

  require Logger

  @type t() :: %__MODULE__{}
  defstruct [
    :node_base_name,
    :service_account_path,
    :cluster_domain,
    :namespace,
    :ip_lookup_mode,
    :cluster_name,
    :service_name,
    :selector,
    :master_name,
    :master_port,
    :host_identifier
  ]

  defimpl DiscoveryProtocol, for: Flezha.Cluster.K8sPodDiscovery do
    @impl true
    def get_nodes(settings) do
      service_account_path = settings.service_account_path
      token = get_token(service_account_path)
      ssl_opts = get_ssl_opts(service_account_path)
      namespace = get_namespace(service_account_path, settings.namespace)

      master =
        cond do
          String.ends_with?(settings.master_name, settings.cluster_domain) ->
            settings.master_name

          String.ends_with?(settings.master_name, ".") ->
            # The dot at the end is used to determine that the name is "final"
            settings.master_name

          :else ->
            settings.master_name <> "." <> settings.cluster_domain
        end

      cond do
        settings.node_base_name != nil and settings.selector != nil ->
          selector = URI.encode(settings.selector)

          path =
            case settings.ip_lookup_mode do
              :endpoints -> "api/v1/namespaces/#{namespace}/endpoints?labelSelector=#{selector}"
              :pods -> "api/v1/namespaces/#{namespace}/pods?labelSelector=#{selector}"
            end

          headers = [{~c"authorization", ~c"Bearer #{token}"}]
          http_options = [ssl: ssl_opts, timeout: 15000]

          request =
            :httpc.request(
              :get,
              {~c"https://#{master}:#{settings.master_port}/#{path}", headers},
              http_options,
              []
            )

          # TODO: port should be configurable.
          nodes =
            case request do
              {:ok, {{_version, 200, _status}, _headers, body}} ->
                parse_response(settings.ip_lookup_mode, Jason.decode!(body))
                |> Enum.map(fn node_info ->
                  format_node(
                    settings.host_identifier,
                    node_info,
                    settings.node_base_name,
                    settings.cluster_name,
                    settings.service_name
                  )
                end)

              {:ok, {{_version, 403, _status}, _headers, body}} ->
                %{"message" => msg} = Jason.decode!(body)
                Logger.warning("cannot query kubernetes (unauthorized): #{msg}")
                []

              {:ok, {{_version, code, status}, _headers, body}} ->
                Logger.warning("cannot query kubernetes (#{code} #{status}): #{inspect(body)}")
                []

              {:error, reason} ->
                Logger.error("request to kubernetes failed!: #{inspect(reason)}")
                []
            end

          {:ok, settings, nodes}
      end
    end

    # TODO: add other field validations here.
    @impl true
    def validate_settings(_settings) do
      with :ok <- validate_service_account_path(),
           :ok <- validate_cluster_domain(),
           :ok <- validate_pod_ip_lookup() do
        {:ok, __MODULE__}
      else
        {:error, reason} -> {:error, reason}
      end
    end

    defp validate_pod_ip_lookup() do
      ip_lookup_mode = Application.fetch_env!(:flezha, :k8s_ip_lookup_mode)

      cond do
        ip_lookup_mode in [:endpoints, :pods] ->
          :ok

        true ->
          {:error, "Invalid Pod IP lookup mode: #{inspect(ip_lookup_mode)}"}
      end
    end

    defp validate_service_account_path() do
      service_account_path = Application.fetch_env!(:flezha, :k8s_service_account_path)

      cond do
        File.dir?(service_account_path) ->
          :ok

        true ->
          {:error, "Service account path does not exist: #{service_account_path}"}
      end
    end

    defp validate_cluster_domain() do
      cluster_domain = Application.fetch_env!(:flezha, :k8s_cluster_domain)

      cond do
        String.valid?(cluster_domain) && String.length(cluster_domain) > 0 ->
          :ok

        true ->
          {:error, "Invalid or empty cluster domain: #{cluster_domain}"}
      end
    end

    @spec get_token(String.t()) :: String.t()
    defp get_token(service_account_path) do
      path = Path.join(service_account_path, "token")

      case File.exists?(path) do
        true -> path |> File.read!() |> String.trim()
        false -> ""
      end
    end

    @spec get_ssl_opts(Path.t()) :: Keyword.t()
    defp get_ssl_opts(service_account_path) do
      path = Path.join(service_account_path, "ca.crt")

      case File.exists?(path) do
        true ->
          [
            verify: :verify_peer,
            cacertfile: String.to_charlist(path)
          ]

        false ->
          [verify: :verify_none]
      end
    end

    defp get_namespace(service_account_path, nil) do
      path = Path.join(service_account_path, "namespace")

      if File.exists?(path) do
        path |> File.read!() |> String.trim()
      else
        ""
      end
    end

    defp get_namespace(_, namespace), do: namespace

    defp format_node(:ip, %{ip: ip}, app_name, _cluster_name, _service_name),
      do: :"#{app_name}@#{ip}"

    defp format_node(
           :hostname,
           %{hostname: hostname, namespace: namespace},
           app_name,
           cluster_name,
           service_name
         ) do
      :"#{app_name}@#{hostname}.#{service_name}.#{namespace}.svc.#{cluster_name}.local"
    end

    defp format_node(:dns, %{ip: ip, namespace: namespace}, app_name, cluster_name, _service_name) do
      ip = String.replace(ip, ".", "-")
      :"#{app_name}@#{ip}.#{namespace}.pod.#{cluster_name}.local"
    end

    defp parse_response(:endpoints, resp) do
      case resp do
        %{"items" => items} when is_list(items) ->
          Enum.reduce(items, [], fn
            %{"subsets" => subsets}, acc when is_list(subsets) ->
              addrs =
                Enum.flat_map(subsets, fn
                  %{"addresses" => addresses} when is_list(addresses) ->
                    Enum.map(addresses, fn %{
                                             "ip" => ip,
                                             "targetRef" => %{"namespace" => namespace}
                                           } =
                                             address ->
                      %{ip: ip, namespace: namespace, hostname: address["hostname"]}
                    end)

                  _ ->
                    []
                end)

              acc ++ addrs

            _, acc ->
              acc
          end)

        _ ->
          []
      end
    end

    defp parse_response(:pods, resp) do
      case resp do
        %{"items" => items} when is_list(items) ->
          Enum.map(items, fn
            %{
              "status" => %{"podIP" => ip},
              "metadata" => %{"namespace" => ns},
              "spec" => pod_spec
            } ->
              %{ip: ip, namespace: ns, hostname: pod_spec["hostname"]}

            _ ->
              nil
          end)
          |> Enum.filter(&(&1 != nil))

        _ ->
          []
      end
    end
  end
end
