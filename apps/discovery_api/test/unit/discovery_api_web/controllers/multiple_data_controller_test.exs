defmodule DiscoveryApiWeb.MultipleDataControllerTest do
  use DiscoveryApiWeb.ConnCase
  use Placebo
  alias DiscoveryApi.Data.Model
  alias DiscoveryApi.Services.PrestoService
  alias DiscoveryApiWeb.Utilities.QueryAccessUtils
  alias DiscoveryApiWeb.Utilities.ModelAccessUtils

  setup do
    public_one_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: false,
        systemName: "public__one"
      })

    public_two_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: false,
        systemName: "public__two"
      })

    private_one_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: true,
        systemName: "private__one"
      })

    private_two_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: true,
        systemName: "private__two"
      })

    coda_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: false,
        systemName: "coda__test_dataset"
      })

    geojson_dataset =
      DiscoveryApi.Test.Helper.sample_model(%{
        private: false,
        systemName: "geojson__geojson"
      })

    datasets = [
      public_one_dataset,
      public_two_dataset,
      private_one_dataset,
      private_two_dataset,
      coda_dataset,
      geojson_dataset
    ]

    allow(Model.get_all(), return: datasets, meck_options: [:passthrough])

    allow(Prestige.new_session(any()), return: :connection)

    {
      :ok,
      %{
        public_tables: [public_one_dataset, public_two_dataset] |> Enum.map(&Map.get(&1, :systemName)),
        private_tables: [private_one_dataset, private_two_dataset] |> Enum.map(&Map.get(&1, :systemName))
      }
    }
  end

  @moduletag capture_log: true
  describe "query multiple datasets" do
    setup do
      json_from_execute = [
        %{"a" => 2, "b" => 2},
        %{"a" => 3, "b" => 3},
        %{"a" => 1, "b" => 1}
      ]

      csv_from_execute = "a,b\n2,2\n3,3\n1,1\n"

      {
        :ok,
        %{
          json_response: json_from_execute,
          csv_response: csv_from_execute
        }
      }
    end

    test "can select from some public datasets as json", %{
      conn: conn,
      public_tables: public_tables,
      json_response: expected_response
    } do
      statement = """
        WITH public_one AS (select a from public__one), public_two AS (select b from public__two)
        SELECT * FROM public_one JOIN public_two ON public_one.a = public_two.b
      """

      allow(Prestige.stream!(any(), any()), return: [:result])
      allow(Prestige.Result.as_maps(:result), return: expected_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, public_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      response_body =
        conn
        |> put_req_header("accept", "application/json")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query", statement)
        |> response(200)
        |> Jason.decode!()

      assert expected_response == response_body
    end

    test "can select from some public datasets as csv", %{
      conn: conn,
      public_tables: public_tables,
      json_response: allowed_response,
      csv_response: expected_response
    } do
      statement = """
        WITH public_one AS (select a from public__one), public_two AS (select b from public__two)
        SELECT * FROM public_one JOIN public_two ON public_one.a = public_two.b
      """

      allow(Prestige.stream!(any(), any()), return: [:result])
      allow(Prestige.Result.as_maps(:result), return: allowed_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, public_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      response_body =
        conn
        |> put_req_header("accept", "text/csv")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query", statement)
        |> response(200)

      assert expected_response == response_body
    end

    test "can describe queries for some public datasets", %{
      conn: conn,
      public_tables: public_tables
    } do
      statement = """
        WITH public_one AS (select a from public__one), public_two AS (select b from public__two)
        SELECT * FROM public_one JOIN public_two ON public_one.a = public_two.b
      """

      allowed_response = [
        %{
          "Column Name" => "a",
          "Type" => "integer"
        },
        %{
          "Column Name" => "b",
          "Type" => "integer"
        }
      ]

      expected_response =
        [
          %{
            name: "a",
            type: "integer"
          },
          %{
            name: "b",
            type: "integer"
          }
        ]
        |> Jason.encode!()

      allow(Prestige.prepare!(any(), any(), any()), return: [:result])
      allow(Prestige.execute!(any(), any()), return: :result)
      allow(Prestige.Result.as_maps(:result), return: allowed_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, public_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      response_body =
        conn
        |> put_req_header("accept", "application/json")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query/describe", statement)
        |> response(200)

      assert expected_response == response_body
    end

    test "can select from some authorized private datasets", %{
      conn: conn,
      private_tables: private_tables,
      json_response: allowed_response
    } do
      statement = """
        WITH private_one AS (select a from private__one), private_two AS (select b from private__two)
        SELECT * FROM private_one JOIN private_two ON private_one.a = private_two.b
      """

      allow(Prestige.stream!(any(), any()), return: [:result])
      allow(Prestige.Result.as_maps(:result), return: allowed_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, private_tables})
      allow(QueryAccessUtils.authorized_to_query?(any(), any()), return: true, meck_options: [:passthrough])

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(200)
    end

    test "can describe queries for some authorized private datasets", %{
      conn: conn,
      private_tables: private_tables
    } do
      statement = """
      WITH private_one AS (select a from private__one), private_two AS (select b from private__two)
      SELECT * FROM private_one JOIN private_two ON private_one.a = private_two.b
      """

      allowed_response = [
        %{
          "Column Name" => "a",
          "Type" => "integer"
        },
        %{
          "Column Name" => "b",
          "Type" => "integer"
        }
      ]

      expected_response =
        [
          %{
            name: "a",
            type: "integer"
          },
          %{
            name: "b",
            type: "integer"
          }
        ]
        |> Jason.encode!()

      allow(Prestige.prepare!(any(), any(), any()), return: [:result])
      allow(Prestige.execute!(any(), any()), return: :result)
      allow(Prestige.Result.as_maps(:result), return: allowed_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, private_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      response_body =
        conn
        |> put_req_header("accept", "application/json")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query/describe", statement)
        |> response(200)

      assert expected_response == response_body
    end

    test "can't select from some unauthorized private datasets", %{
      conn: conn,
      private_tables: private_tables,
      json_response: allowed_response
    } do
      statement = """
        WITH private_one AS (select a from private__one), private_two AS (select b from private__two)
        SELECT * FROM private_one JOIN private_two ON private_one.a = private_two.b
      """

      allow(Prestige.query!(any(), any()), return: :result)
      allow(Prestige.Result.as_maps(:result), return: allowed_response)
      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, private_tables})
      allow(QueryAccessUtils.authorized_to_query?(any(), any()), seq: [false, true])

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(400)
    end

    test "can't describe queries for unauthorized private datasets", %{
      conn: conn,
      private_tables: private_tables
    } do
      statement = """
      WITH private_one AS (select a from private__one), private_two AS (select b from private__two)
      SELECT * FROM private_one JOIN private_two ON private_one.a = private_two.b
      """

      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, private_tables})
      allow(QueryAccessUtils.authorized_to_query?(any(), any()), seq: [false, true])

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(400)
    end

    test "can't perform or describe query if there is an error getting affected tables", %{conn: conn} do
      statement = """
        INSERT INTO public__one SELECT * FROM public__two
      """

      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:error, :does_not_matter})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(400)

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query/describe", statement)
             |> response(400)
    end

    test "unable to query or describe datasets which are not in redis", %{conn: conn} do
      statement = """
      SELECT * FROM not_in_redis
      """

      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, ["not_in_redis"]})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      _response_body =
        conn
        |> put_req_header("accept", "text/csv")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query", statement)
        |> response(400)

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query/describe", statement)
             |> response(400)

      assert not called?(Prestige.query!(any(), any()))
    end

    test "can't perform or describe query if it not a supported/allowed statement type", %{conn: conn, public_tables: public_tables} do
      statement = """
        EXPLAIN ANALYZE select * from public__one
      """

      allow(Prestige.query!(any(), any()), return: :result)
      allow(PrestoService.is_select_statement?(statement), return: false)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, public_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(400)

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query/describe", statement)
             |> response(400)
    end

    test "does not accept requests with no statement in the body", %{conn: conn} do
      statement = ""

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query", statement)
             |> response(400)

      assert conn
             |> put_req_header("accept", "application/json")
             |> put_req_header("content-type", "text/plain")
             |> post("/api/v1/query/describe", statement)
             |> response(400)
    end

    test "returns prestige error details if prestige throws", %{conn: conn, public_tables: public_tables} do
      statement = "select quantity*2131241224124412124 from public__one"
      failure_message = "bigint multiplication overflow: 7694 * 2131241224124412124"
      expected_response = "{\"message\":\"#{failure_message}\"}"

      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, public_tables})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])
      allow(Prestige.stream!(any(), any()), exec: fn _, _ -> raise Prestige.Error, failure_message end)

      assert expected_response ==
               conn
               |> put_req_header("accept", "application/json")
               |> put_req_header("content-type", "text/plain")
               |> post("/api/v1/query", statement)
               |> response(400)
    end
  end

  describe "query geojson" do
    setup do
      statement = "SELECT * FROM geojson__geojson"

      allow(Prestige.stream!(any(), any()), return: [:result])

      allow(Prestige.Result.as_maps(:result),
        return: [
          %{"feature" => "{\"geometry\": {\"coordinates\": [1, 0]}}"},
          %{"feature" => "{\"geometry\": {\"coordinates\": [[0, 1]]}}"}
        ]
      )

      allow(PrestoService.is_select_statement?(statement), return: true)
      allow(PrestoService.get_affected_tables(any(), statement), return: {:ok, ["geojson__geojson"]})
      allow(ModelAccessUtils.has_access?(any(), any()), return: true, meck_options: [:passthrough])

      %{statement: statement}
    end

    test "returns geojson with bounding box", %{conn: conn, statement: statement} do
      actual =
        conn
        |> put_req_header("accept", "application/json")
        |> put_req_header("content-type", "text/plain")
        |> post("/api/v1/query?_format=geojson", statement)
        |> response(200)

      assert Jason.decode!(actual) == %{
               "type" => "FeatureCollection",
               "bbox" => [0, 0, 1, 1],
               "features" => [
                 %{
                   "geometry" => %{
                     "coordinates" => [1, 0]
                   }
                 },
                 %{
                   "geometry" => %{
                     "coordinates" => [[0, 1]]
                   }
                 }
               ]
             }
    end
  end
end
