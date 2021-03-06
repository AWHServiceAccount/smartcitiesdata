defmodule Reaper.DataExtract.ProcessorTest do
  use ExUnit.Case
  use Placebo
  import ExUnit.CaptureLog
  import Mox

  alias Reaper.{Cache, Persistence}
  alias Reaper.DataExtract.Processor

  alias SmartCity.TestDataGenerator, as: TDG

  @dataset_id "12345-6789"

  @csv """
  one,two,three
  four,five,six
  """

  @download_dir System.get_env("TMPDIR") || "/tmp/"
  use TempEnv, reaper: [download_dir: @download_dir]

  setup do
    {:ok, horde_registry} = Horde.Registry.start_link(keys: :unique, name: Reaper.Cache.Registry)
    {:ok, horde_sup} = Horde.DynamicSupervisor.start_link(strategy: :one_for_one, name: Reaper.Horde.Supervisor)

    on_exit(fn ->
      kill(horde_sup)
      kill(horde_registry)
    end)

    bypass = Bypass.open()

    Bypass.expect(bypass, "GET", "/api/csv", fn conn ->
      Plug.Conn.resp(conn, 200, @csv)
    end)

    dataset =
      TDG.create_dataset(
        id: @dataset_id,
        technical: %{
          sourceType: "ingest",
          sourceFormat: "csv",
          sourceUrl: "http://localhost:#{bypass.port}/api/csv",
          cadence: 100,
          schema: [
            %{name: "a", type: "string"},
            %{name: "b", type: "string"},
            %{name: "c", type: "string"}
          ],
          allow_duplicates: false
        }
      )

    allow Elsa.create_topic(any(), any()), return: :ok
    allow Elsa.Supervisor.start_link(any()), return: {:ok, :pid}
    allow Elsa.topic?(any(), any()), return: true
    allow :brod.get_producer(any(), any(), any()), return: {:ok, :pid}

    [bypass: bypass, dataset: dataset]
  end

  describe "process/2 happy path" do
    setup do
      allow Elsa.produce(any(), any(), any(), any()), return: :ok
      allow Persistence.remove_last_processed_index(@dataset_id), return: :ok

      :ok
    end

    test "parses turns csv into data messages and sends to kafka", %{dataset: dataset} do
      allow Persistence.get_last_processed_index(@dataset_id), return: -1
      allow Persistence.record_last_processed_index(@dataset_id, any()), return: "OK"

      Processor.process(dataset)

      messages = capture(1, Elsa.produce(any(), any(), any(), any()), 3)

      expected = [
        %{"a" => "one", "b" => "two", "c" => "three"},
        %{"a" => "four", "b" => "five", "c" => "six"}
      ]

      assert expected == get_payloads(messages)

      assert_called Persistence.record_last_processed_index(any(), any()), once()
      assert_called Persistence.remove_last_processed_index(@dataset_id), once()
    end

    test "eliminates duplicates before sending to kafka", %{dataset: dataset} do
      allow Persistence.get_last_processed_index(@dataset_id), return: -1
      allow Persistence.record_last_processed_index(@dataset_id, any()), return: "OK"
      Horde.DynamicSupervisor.start_child(Reaper.Horde.Supervisor, {Reaper.Cache, name: dataset.id})
      Cache.cache(dataset.id, %{"a" => "one", "b" => "two", "c" => "three"})

      Processor.process(dataset)

      messages = capture(1, Elsa.produce(any(), any(), any(), any()), 3)
      assert [%{"a" => "four", "b" => "five", "c" => "six"}] == get_payloads(messages)
      assert_called Elsa.produce(any(), any(), any(), any()), once()

      assert_called Persistence.record_last_processed_index(any(), any()), once()
      assert_called Persistence.remove_last_processed_index(@dataset_id), once()
    end
  end

  @tag capture_log: true
  test "process/2 should remove file for dataset regardless of error being raised", %{dataset: dataset} do
    allow Persistence.get_last_processed_index(any()), return: -1

    allow Reaper.Cache.mark_duplicates(any(), any()),
      exec: fn _, _ -> raise "some error" end,
      meck_options: [:passthrough]

    assert_raise RuntimeError, fn ->
      Processor.process(dataset)
    end

    assert false == File.exists?(@download_dir <> dataset.id)
  end

  test "process/2 should catch log all exceptions and reraise", %{dataset: dataset} do
    allow Persistence.get_last_processed_index(any()), return: -1

    allow Elsa.produce(any(), any(), any(), any()), exec: fn _, _, _, _ -> raise "some error" end

    log =
      capture_log(fn ->
        assert_raise RuntimeError, fn ->
          Processor.process(dataset)
        end
      end)

    assert log =~ inspect(dataset)
    assert log =~ "some error"
  end

  test "process/2 should execute providers prior to processing", %{bypass: bypass} do
    dataset_id = "prov-dataset-1234"
    allow Elsa.produce(any(), any(), any(), any()), return: :ok
    allow Persistence.remove_last_processed_index(dataset_id), return: :ok
    allow Persistence.get_last_processed_index(dataset_id), return: -1
    allow Persistence.record_last_processed_index(dataset_id, any()), return: "OK"

    Providers.Echo
    |> expect(:provide, fn _, %{value: value} -> value end)

    provisioned_dataset = TDG.create_dataset(
        id: "prov-dataset-1234",
        technical: %{
          sourceType: "ingest",
          sourceFormat: "csv",
          sourceUrl: "http://localhost:#{bypass.port}/api/csv",
          cadence: 100,
          schema: [
            %{name: "a", type: "string"},
            %{name: "b", type: "string"},
            %{name: "c", type: "string"},
            %{name: "p", type: "string", default: %{
              provider: "Echo",
              opts: %{value: "six of six"},
              version: "1"
            }}
          ],
          allow_duplicates: false
        }
      )

    Processor.process(provisioned_dataset)

    messages = capture(1, Elsa.produce(any(), any(), any(), any()), 3)

    expected = [
      %{"a" => "one", "b" => "two", "c" => "three", "p" => "six of six"},
      %{"a" => "four", "b" => "five", "c" => "six", "p" => "six of six"}
    ]

    assert expected == get_payloads(messages)
  end

  defp get_payloads(list) do
    list
    |> Enum.map(&SmartCity.Data.new/1)
    |> Enum.map(fn {:ok, data} -> data.payload end)
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
