defmodule Forklift.Application do
  @moduledoc false

  use Application
  require Logger

  def start(_type, _args) do
    Forklift.MetricsExporter.setup()

    children =
      [
        libcluster(),
        redis(),
        metrics(),
        {DynamicSupervisor, strategy: :one_for_one, name: Forklift.Dynamic.Supervisor},
        Forklift.Quantum.Scheduler,
        {Brook, Application.get_env(:forklift, :brook)},
        migrations(),
        Forklift.InitServer
      ]
      |> List.flatten()

    if Application.get_env(:forklift, :table_writer) == Pipeline.Writer.S3Writer do
      fetch_and_set_s3_credentials()
    end

    opts = [strategy: :one_for_one, name: Forklift.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def redis_client(), do: :redix

  defp redis do
    case Application.get_env(:redix, :args) do
      nil -> []
      redix_args -> {Redix, Keyword.put(redix_args, :name, redis_client())}
    end
  end

  defp migrations do
    case Application.get_env(:redix, :args) do
      nil -> []
      _args -> Forklift.Migrations
    end
  end

  defp metrics() do
    case Application.get_env(:forklift, :metrics_port) do
      nil ->
        []

      metrics_port ->
        Plug.Cowboy.child_spec(
          scheme: :http,
          plug: Forklift.MetricsExporter,
          options: [port: metrics_port]
        )
    end
  end

  defp libcluster do
    case Application.get_env(:libcluster, :topologies) do
      nil -> []
      topology -> {Cluster.Supervisor, [topology, [name: Cluster.ClusterSupervisor]]}
    end
  end

  defp fetch_and_set_s3_credentials() do
    endpoint = Application.get_env(:forklift, :secrets_endpoint)

    if is_nil(endpoint) || String.length(endpoint) == 0 do
      Logger.warn(
        "No secrets endpoint. Forklift will need to explicitly define a secret id and key to interact with the object store"
      )

      []
    else
      case Forklift.SecretRetriever.retrieve_objectstore_keys() do
        nil ->
          raise RuntimeError, message: "Could not start application, failed to retrieve credentials from storage"

        {:error, error} ->
          raise RuntimeError,
            message: "Could not start application, encountered error while retrieving credentials: #{error}"

        {:ok, creds} ->
          Application.put_env(:ex_aws, :access_key_id, Map.get(creds, "aws_access_key_id"))
          Application.put_env(:ex_aws, :secret_access_key, Map.get(creds, "aws_secret_access_key"))
      end
    end
  end
end
