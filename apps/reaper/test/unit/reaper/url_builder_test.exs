defmodule Reaper.UrlBuilderTest do
  use ExUnit.Case
  use Placebo
  import Checkov
  alias Reaper.{ReaperConfig, UrlBuilder}

  data_test "builds #{result}" do
    %ReaperConfig{sourceUrl: transformed_url} = UrlBuilder.build(reaper_config)
    assert result == transformed_url

    where([
      [:reaper_config, :result],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{start_date: "19700101", end_date: "19700102"}
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{
            start_date: "<%= Date.to_iso8601(~D[1970-01-01], :basic) %>",
            end_date: "<%= Date.to_iso8601(~D[1970-01-02], :basic) %>"
          }
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{}
        }),
        "https://my-url.com"
      ],
      [
        FixtureHelper.new_reaper_config(%{
          id: "",
          sourceUrl: "https://my-url.com",
          queryParams: %{
            start_date:
              "<%= Date.to_iso8601(last_success_time || DateTime.from_unix!(0) |> DateTime.to_date(), :basic) %>",
            end_date: "<%= Date.to_iso8601(~D[1970-01-02], :basic) %>"
          }
        }),
        "https://my-url.com?end_date=19700102&start_date=19700101"
      ]
    ])
  end
end
