defmodule Andi.DatasetMigrationTest do
  use ExUnit.Case
  use Divo, auto_start: false

  import SmartCity.TestHelper
  alias SmartCity.TestDataGenerator, as: TDG

  require Andi
  @instance Andi.instance_name()

  @tag :capture_log
  test "should run the modified date migration" do
    Application.ensure_all_started(:redix)
    Application.ensure_all_started(:faker)

    {:ok, redix} = Redix.start_link(Keyword.put(Application.get_env(:redix, :args), :name, :redix))
    Process.unlink(redix)

    {:ok, brook} =
      Brook.start_link(
        Application.get_env(:andi, :brook)
        |> Keyword.delete(:driver)
        |> Keyword.put(:instance, @instance)
      )

    Process.unlink(brook)

    dataset_with_proper_modified_date_id = 1
    dataset_bad_modified_date_id = 2
    invalid_dataset_id = 3

    good_date = "2017-08-08T13:03:48.000Z"
    bad_date = "Jan 13, 2018"
    transformed_date = "2018-01-13T00:00:00Z"

    Brook.Test.with_event(
      @instance,
      Brook.Event.new(type: "andi_config:migration", author: "migration", data: %{}),
      fn ->
        Brook.ViewState.merge(
          :dataset,
          dataset_with_proper_modified_date_id,
          TDG.create_dataset(id: dataset_with_proper_modified_date_id, business: %{modifiedDate: good_date})
        )

        Brook.ViewState.merge(
          :dataset,
          dataset_bad_modified_date_id,
          TDG.create_dataset(id: dataset_bad_modified_date_id, business: %{modifiedDate: bad_date})
        )

        Brook.ViewState.merge(:dataset, invalid_dataset_id, %{})
      end
    )

    kill(brook)
    kill(redix)

    Application.ensure_all_started(:andi)

    Process.sleep(10_000)

    eventually(fn ->
      # Migration has recorded a succesful completion
      assert true == Brook.get!(@instance, :migration, "modified_date_migration_completed")

      # Good date was not modified
      assert good_date == get_modified_date_from_brook(dataset_with_proper_modified_date_id)

      # Bad date was properly transformed into a good date
      assert transformed_date == get_modified_date_from_brook(dataset_bad_modified_date_id)
    end)

    Application.stop(:andi)
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
  end

  defp get_modified_date_from_brook(id) do
    case Brook.get!(@instance, :dataset, id) do
      nil -> nil
      result -> result.business.modifiedDate
    end
  end
end
