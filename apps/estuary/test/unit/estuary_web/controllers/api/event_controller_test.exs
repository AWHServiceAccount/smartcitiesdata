defmodule EstuaryWeb.API.EventControllerTest do
  use EstuaryWeb.ConnCase
  use Placebo
  alias Estuary.Services.EventRetrievalService

  describe "GET events from /api/v1/events" do
    @tag capture_log: true
    test "should return a 200 and the events when events are found in the database", %{conn: conn} do
      events = [
        %{
          "author" => "Author-2020-01-21 23:29:20.171519Z",
          "create_ts" => 1_579_649_360,
          "data" => "Data-2020-01-21 23:29:20.171538Z",
          "type" => "Type-2020-01-21 23:29:20.171543Z"
        },
        %{
          "author" => "Author-2020-01-21 23:25:52.522084Z",
          "create_ts" => 1_579_649_152,
          "data" => "Data-2020-01-21 23:25:52.522107Z",
          "type" => "Type-2020-01-21 23:25:52.522111Z"
        }
      ]

      expected_events =
        "[{\"author\":\"Author-2020-01-21 23:29:20.171519Z\",\"create_ts\":1579649360,\"data\":\"Data-2020-01-21 23:29:20.171538Z\",\"type\":\"Type-2020-01-21 23:29:20.171543Z\"},{\"author\":\"Author-2020-01-21 23:25:52.522084Z\",\"create_ts\":1579649152,\"data\":\"Data-2020-01-21 23:25:52.522107Z\",\"type\":\"Type-2020-01-21 23:25:52.522111Z\"}]"

      allow(EventRetrievalService.get_all(), return: {:ok, events})

      conn = get(conn, "/api/v1/events")
      actual_events = conn.resp_body
      assert expected_events == actual_events
    end

    @tag capture_log: true
    test "should return 404 and message when error occurs", %{conn: conn} do
      expected_error = "Unable to process your request"
      allow(EventRetrievalService.get_all(), return: {:error, :do_not_care})
      conn = get(conn, "/api/v1/events")

      actual_error =
        conn
        |> json_response(404)

      assert expected_error == actual_error
    end
  end
end
