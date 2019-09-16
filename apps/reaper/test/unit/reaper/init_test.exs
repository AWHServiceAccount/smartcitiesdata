defmodule Reaper.InitTest do
  use ExUnit.Case
  use Placebo

  alias SmartCity.TestDataGenerator, as: TDG
  alias Reaper.Collections.{Extractions, FileIngestions}
  import SmartCity.TestHelper

  setup do
    {:ok, horde_supervisor} = Horde.Supervisor.start_link(name: Reaper.Horde.Supervisor, strategy: :one_for_one)
    {:ok, reaper_horde_registry} = Reaper.Horde.Registry.start_link(name: Reaper.Horde.Registry, keys: :unique)
    {:ok, brook} = Brook.start_link(Application.get_env(:reaper, :brook))

    on_exit(fn ->
      kill(brook)
      kill(reaper_horde_registry)
      kill(horde_supervisor)
    end)

    :ok
  end

  describe "Extractions" do
    test "starts all extract processes that should be running" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(fn ->
        Extractions.update_dataset(dataset)
        Extractions.update_last_fetched_timestamp(dataset.id, nil)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.DataExtract.Processor.process(dataset)
      end)
    end

    test "does not start successfully completed extract processes" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(fn ->
        Extractions.update_dataset(dataset, start_time)
        Extractions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      refute_called Reaper.DataExtract.Processor.process(dataset)
    end

    test "starts data extract process when started_timestamp > last_fetched_timestamp" do
      allow Reaper.DataExtract.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(-3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "ingest"})

      Brook.Test.with_event(fn ->
        Extractions.update_dataset(dataset, start_time)
        Extractions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.DataExtract.Processor.process(dataset)
      end)
    end
  end

  describe "Ingestions" do
    test "starts all ingest processes that should be running" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(fn ->
        FileIngestions.update_dataset(dataset)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.FileIngest.Processor.process(dataset)
      end)
    end

    test "does not start successfully completed ingest processes" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(fn ->
        FileIngestions.update_dataset(dataset, start_time)
        FileIngestions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      refute_called Reaper.FileIngest.Processor.process(dataset)
    end

    test "starts data extract process when started_timestamp > last_fetched_timestamp" do
      allow Reaper.FileIngest.Processor.process(any()), return: :ok

      start_time = DateTime.utc_now()
      end_time = start_time |> DateTime.add(-3600, :second)

      dataset = TDG.create_dataset(id: "ds1", technical: %{sourceType: "host"})

      Brook.Test.with_event(fn ->
        FileIngestions.update_dataset(dataset, start_time)
        FileIngestions.update_last_fetched_timestamp(dataset.id, end_time)
      end)

      Reaper.Init.run()

      eventually(fn ->
        assert_called Reaper.FileIngest.Processor.process(dataset)
      end)
    end
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
