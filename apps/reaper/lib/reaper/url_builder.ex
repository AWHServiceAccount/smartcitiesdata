defmodule Reaper.UrlBuilder do
  alias Reaper.ReaperConfig
  @moduledoc false
  def build(%ReaperConfig{sourceUrl: url, queryParams: query_params} = reaper_config)
      when query_params == %{},
      do: reaper_config

  def build(%ReaperConfig{sourceUrl: url, queryParams: query_params} = reaper_config) do
    last_success_time = extract_last_success_time(reaper_config)

    string_params =
      query_params
      |> evaluate_parameters(last_success_time: last_success_time)
      |> URI.encode_query()

    %{reaper_config | sourceUrl: "#{url}?#{string_params}"}
  end

  defp extract_last_success_time(reaper_config) do
    case reaper_config.lastSuccessTime do
      nil -> false
      _time -> convert_timestamp(reaper_config.lastSuccessTime)
    end
  end

  defp convert_timestamp(timestamp) do
    {:ok, dt, _} = DateTime.from_iso8601(timestamp)
    dt
  end

  defp evaluate_parameters(parameters, bindings) do
    Enum.map(
      parameters,
      &evaluate_parameter(&1, bindings)
    )
  end

  defp evaluate_parameter({key, value}, bindings) do
    {key, EEx.eval_string(value, bindings)}
  end
end
