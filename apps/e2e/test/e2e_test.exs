defmodule E2ETest do
  use ExUnit.Case
  use Divo
  use Placebo
  use Phoenix.ChannelTest

  @moduletag :e2e
  @moduletag capture_log: false
  @endpoint DiscoveryStreamsWeb.Endpoint

  alias SmartCity.TestDataGenerator, as: TDG
  import SmartCity.TestHelper

  @brokers Application.get_env(:e2e, :elsa_brokers)
  @overrides %{
    technical: %{
      orgName: "end_to",
      dataName: "end",
      systemName: "end_to__end",
      schema: [
        %{name: "one", type: "boolean"},
        %{name: "two", type: "string"},
        %{name: "three", type: "integer"}
      ],
      sourceType: "ingest",
      sourceFormat: "text/csv",
      cadence: "once"
    }
  }

  @streaming_overrides %{
    id: "strimmin",
    technical: %{
      dataName: "strimmin",
      orgName: "usa",
      cadence: "*/1 * * * * * *",
      sourceType: "stream",
      systemName: "usa__strimmin"
    }
  }

  @geo_overrides %{
    id: "geo_data",
    technical: %{
      orgName: "end_to",
      dataName: "land",
      systemName: "end_to__land",
      schema: [%{name: "feature", type: "json"}],
      sourceType: "ingest",
      sourceFormat: "zip",
      cadence: "once"
    }
  }

  setup_all do
    Temp.track!()
    Application.put_env(:odo, :working_dir, Temp.mkdir!())
    bypass = Bypass.open()
    shapefile = File.read!("test/support/shapefile.zip")

    Bypass.stub(bypass, "GET", "/path/to/the/data.csv", fn conn ->
      Plug.Conn.resp(conn, 200, "true,foobar,10")
    end)

    Bypass.stub(bypass, "GET", "/path/to/the/geo_data.shapefile", fn conn ->
      Plug.Conn.resp(conn, 200, shapefile)
    end)

    dataset =
      @overrides
      |> put_in(
        [:technical, :sourceUrl],
        "http://localhost:#{bypass.port()}/path/to/the/data.csv"
      )
      |> TDG.create_dataset()

    streaming_dataset = SmartCity.Helpers.deep_merge(dataset, @streaming_overrides)

    geo_dataset =
      @geo_overrides
      |> put_in(
        [:technical, :sourceUrl],
        "http://localhost:#{bypass.port()}/path/to/the/geo_data.shapefile"
      )
      |> TDG.create_dataset()

    [dataset: dataset, streaming_dataset: streaming_dataset, geo_dataset: geo_dataset]
  end

  describe "creating an organization" do
    test "via RESTful POST" do
      org = TDG.create_organization(%{orgName: "end_to", id: "org-id"})

      resp =
        HTTPoison.post!("http://localhost:4000/api/v1/organization", Jason.encode!(org), [
          {"Content-Type", "application/json"}
        ])

      assert resp.status_code == 201
    end

    test "persists the organization for downstream use" do
      base = Application.get_env(:paddle, Paddle)[:base]

      eventually(fn ->
        with resp <- HTTPoison.get!("http://localhost:4000/api/v1/organizations"),
             [org] <- Jason.decode!(resp.body) do
          assert org["dn"] == "cn=end_to,ou=integration,#{base}"
          assert org["id"] == "org-id"
          assert org["orgName"] == "end_to"
        end
      end)
    end
  end

  describe "creating a dataset" do
    test "via RESTful PUT", %{dataset: ds} do
      resp =
        HTTPoison.put!("http://localhost:4000/api/v1/dataset", Jason.encode!(ds), [
          {"Content-Type", "application/json"}
        ])

      assert resp.status_code == 201
    end

    test "creates a PrestoDB table" do
      expected = [
        %{"Column" => "one", "Comment" => "", "Extra" => "", "Type" => "boolean"},
        %{"Column" => "two", "Comment" => "", "Extra" => "", "Type" => "varchar"},
        %{"Column" => "three", "Comment" => "", "Extra" => "", "Type" => "integer"}
      ]

      eventually(
        fn ->
          table = query("describe hive.default.end_to__end", true)

          assert table == expected
        end,
        500,
        20
      )
    end

    test "stores a definition that can be retrieved", %{dataset: expected} do
      resp = HTTPoison.get!("http://localhost:4000/api/v1/datasets")
      assert resp.body == Jason.encode!([expected])
    end
  end

  # This series of tests should be extended as more apps are added to the umbrella.
  describe "ingested data" do
    test "is written by reaper", %{dataset: ds} do
      topic = "#{Application.get_env(:reaper, :output_topic_prefix)}-#{ds.id}"

      eventually(fn ->
        {:ok, _, [message]} = Elsa.fetch(@brokers, topic)
        {:ok, data} = SmartCity.Data.new(message.value)

        assert %{"one" => "true", "two" => "foobar", "three" => "10"} == data.payload
      end)
    end

    test "is standardized by valkyrie", %{dataset: ds} do
      topic = "#{Application.get_env(:valkyrie, :output_topic_prefix)}-#{ds.id}"

      eventually(fn ->
        {:ok, _, [message]} = Elsa.fetch(@brokers, topic)
        {:ok, data} = SmartCity.Data.new(message.value)

        assert %{"one" => true, "two" => "foobar", "three" => 10} == data.payload
      end)
    end

    @tag timeout: :infinity, capture_log: true
    test "persists in PrestoDB", %{dataset: ds} do
      topic = "#{Application.get_env(:forklift, :input_topic_prefix)}-#{ds.id}"
      table = ds.technical.systemName

      eventually(fn ->
        assert Elsa.topic?(@brokers, topic)
      end)

      eventually(fn ->
        assert :ok = Forklift.DataWriter.compact_dataset(ds)
      end, 5_000)

      eventually(
        fn ->
          assert [%{"Table" => table}] == query("show tables like '#{table}'", true)

          assert [%{"one" => true, "three" => 10, "two" => "foobar"}] == query(
                   "select * from #{table}",
                   true
                 )
        end,
        10_000
      )
    end

    test "forklift sends event to update last ingested time", %{dataset: _ds} do
      eventually(fn ->
        messages =
          Elsa.Fetch.search_keys(@brokers, "event-stream", "data:write:complete")
          |> Enum.to_list()

        assert 1 == length(messages)
      end)
    end

    test "is profiled by flair", %{dataset: ds} do
      table = Application.get_env(:flair, :table_name_timing)

      expected = ["SmartCityOS", "forklift", "valkyrie", "reaper"]

      eventually(fn ->
        actual = query("select distinct dataset_id, app from #{table}", true)

        Enum.each(expected, fn app -> assert %{"app" => app, "dataset_id" => ds.id} in actual end)
      end)
    end

    test "events have been stored in estuary" do
      table = Application.get_env(:estuary, :table_name)

      eventually(fn ->
        actual = query("SELECT count(1) FROM #{table}", false)
        [row_count] = actual.rows

        assert row_count > 0
      end)
    end
  end

  test "should return status code 200, when estuary is called to get the events" do
    resp = HTTPoison.get!("http://localhost:4010/api/v1/events")

    assert resp.status_code == 200
  end

  describe "streaming data" do
    test "creating a dataset via RESTful PUT", %{streaming_dataset: ds} do
      resp =
        HTTPoison.put!("http://localhost:4000/api/v1/dataset", Jason.encode!(ds), [
          {"Content-Type", "application/json"}
        ])

      assert resp.status_code == 201
    end

    test "is written by reaper", %{streaming_dataset: ds} do
      topic = "#{Application.get_env(:reaper, :output_topic_prefix)}-#{ds.id}"

      eventually(fn ->
        {:ok, _, [message | _]} = Elsa.fetch(@brokers, topic)
        {:ok, data} = SmartCity.Data.new(message.value)

        assert %{"one" => "true", "two" => "foobar", "three" => "10"} == data.payload
      end)
    end

    test "is standardized by valkyrie", %{streaming_dataset: ds} do
      topic = "#{Application.get_env(:valkyrie, :output_topic_prefix)}-#{ds.id}"

      eventually(fn ->
        {:ok, _, [message | _]} = Elsa.fetch(@brokers, topic)
        {:ok, data} = SmartCity.Data.new(message.value)

        assert %{"one" => true, "two" => "foobar", "three" => 10} == data.payload
      end)
    end

    @tag timeout: :infinity, capture_log: true
    test "persists in PrestoDB", %{streaming_dataset: ds} do
      topic = "#{Application.get_env(:forklift, :input_topic_prefix)}-#{ds.id}"
      table = ds.technical.systemName

      eventually(fn ->
        assert Elsa.topic?(@brokers, topic)
      end)

      eventually(fn ->
        assert :ok = Forklift.DataWriter.compact_dataset(ds)
      end, 10_000)

      eventually(
        fn ->
          assert [%{"Table" => table}] == query("show tables like '#{table}'", true)

          assert %{"one" => true, "three" => 10, "two" => "foobar"} in query(
                   "select * from #{table}",
                   true
                 )
        end,
        5_000
      )
    end

    test "is available through socket connection", %{streaming_dataset: ds} do
      eventually(fn ->
        assert "#{Application.get_env(:discovery_streams, :topic_prefix)}#{ds.id}" in DiscoveryStreams.TopicSubscriber.list_subscribed_topics()
      end)

      {:ok, _, _} =
        socket(DiscoveryStreamsWeb.UserSocket, "kenny", %{})
        |> subscribe_and_join(
          DiscoveryStreamsWeb.StreamingChannel,
          "streaming:#{ds.technical.systemName}",
          %{}
        )

      assert_push("update", %{"one" => true, "three" => 10, "two" => "foobar"}, 30_000)
    end

    test "forklift sends event to update last ingested time for streaming datasets", %{
      streaming_dataset: _ds
    } do
      eventually(fn ->
        messages =
          Elsa.Fetch.search_keys(@brokers, "event-stream", "data:write:complete")
          |> Enum.to_list()

        assert length(messages) > 0
      end)
    end

    test "is profiled by flair", %{streaming_dataset: ds} do
      table = Application.get_env(:flair, :table_name_timing)

      expected = ["SmartCityOS", "forklift", "valkyrie", "reaper"]

      eventually(fn ->
        actual = query("select distinct dataset_id, app from #{table}", true)

        Enum.each(expected, fn app -> assert %{"app" => app, "dataset_id" => ds.id} in actual end)
      end)
    end
  end

  describe "geospatial data" do
    test "creating a dataset via RESTful PUT", %{geo_dataset: ds} do
      resp =
        HTTPoison.put!("http://localhost:4000/api/v1/dataset", Jason.encode!(ds), [
          {"Content-Type", "application/json"}
        ])

      assert resp.status_code == 201
    end

    @tag timeout: :infinity, capture_log: true
    test "persists geojson in PrestoDB", %{geo_dataset: ds} do
      table = ds.technical.systemName

      eventually(fn ->
        assert :ok = Forklift.DataWriter.compact_dataset(ds)
      end, 5_000)

      eventually(
        fn ->
          assert [%{"Table" => table}] == query("show tables like '#{table}'", true)

          assert [%{"feature" => actual} | _] = features = query("select * from #{table}", true)

          assert Enum.count(features) <= 88

          result = Jason.decode!(actual)

          assert Map.keys(result) == ["bbox", "geometry", "properties", "type"]

          [coordinates] = result["geometry"]["coordinates"]

          assert Enum.count(coordinates) > 0
        end,
        10_000,
        10
      )
    end
  end

  def query(statment, toggle \\ false)

  def query(statement, false) do
    prestige_session()
    |> Prestige.execute(statement)
    |> case do
      {:ok, result} -> result
      {:error, error} -> {:error, error}
    end
  end

  def query(statement, true) do
    prestige_session()
    |> Prestige.execute(statement)
    |> case do
      {:ok, result} -> Prestige.Result.as_maps(result)
      {:error, error} -> {:error, error}
    end
  end

  defp prestige_session(),
    do: Application.get_env(:prestige, :session_opts) |> Prestige.new_session()
end
