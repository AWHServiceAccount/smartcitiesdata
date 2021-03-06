defmodule Andi.InputSchemas.DatasetInputTest do
  use ExUnit.Case
  import Checkov
  use Placebo
  alias SmartCity.TestDataGenerator, as: TDG

  alias Andi.InputSchemas.DatasetInput
  alias Andi.DatasetCache

  @source_query_param_id Ecto.UUID.generate()
  @source_header_id Ecto.UUID.generate()

  @valid_changes %{
    benefitRating: 0,
    contactEmail: "contact@email.com",
    contactName: "contactName",
    dataName: "dataName",
    dataTitle: "dataTitle",
    description: "description",
    id: "id",
    issuedDate: "2020-01-01T00:00:00Z",
    license: "license",
    orgName: "orgName",
    orgTitle: "orgTitle",
    private: false,
    publishFrequency: "publishFrequency",
    riskRating: 1,
    schema: [%{name: "name", type: "type"}],
    sourceFormat: "sourceFormat",
    sourceHeaders: [
      %{id: Ecto.UUID.generate(), key: "foo", value: "bar"},
      %{id: @source_header_id, key: "fizzle", value: "bizzle"}
    ],
    sourceQueryParams: [
      %{id: Ecto.UUID.generate(), key: "chain", value: "city"},
      %{id: @source_query_param_id, key: "F# minor", value: "add"}
    ],
    sourceType: "sourceType",
    sourceUrl: "https://sourceurl.com?chain=city&F%23+minor=add"
  }

  setup do
    GenServer.call(DatasetCache, :reset)
  end

  describe "light_validation_changeset" do
    data_test "requires value for #{field_name}" do
      changes = @valid_changes |> Map.delete(field_name)

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert changeset.errors == [{field_name, {"is required", [validation: :required]}}]

      where(
        field_name: [
          :benefitRating,
          :contactEmail,
          :contactName,
          :dataName,
          :dataTitle,
          :description,
          :issuedDate,
          :license,
          :orgName,
          :orgTitle,
          :private,
          :publishFrequency,
          :riskRating,
          :sourceFormat,
          :sourceType
        ]
      )
    end

    test "treats empty string values as changes" do
      changes =
        @valid_changes
        |> Map.put(:spatial, "")
        |> Map.put(:temporal, "")

      changeset = DatasetInput.light_validation_changeset(changes)

      assert changeset.valid?
      assert changeset.errors == []
      assert changeset.changes[:spatial] == ""
      assert changeset.changes[:temporal] == ""
    end

    test "requires valid email" do
      changes = @valid_changes |> Map.put(:contactEmail, "nope")

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert changeset.errors == [{:contactEmail, {"has invalid format", [validation: :format]}}]
    end

    data_test "requires #{field_name} be a date" do
      changes = @valid_changes |> Map.put(field_name, "2020-13-32")

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert [{^field_name, _}] = changeset.errors

      where(
        field_name: [
          :issuedDate,
          :modifiedDate
        ]
      )
    end

    data_test "rejects dashes in the #{field_name}" do
      changes = @valid_changes |> Map.put(field_name, "this-has-dashes")

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert changeset.errors == [{field_name, {"cannot contain dashes", [validation: :format]}}]

      where(
        field_name: [
          :orgName,
          :dataName
        ]
      )
    end

    data_test "topLevelSelector is required when sourceFormat is #{source_format}" do
      changes = @valid_changes |> Map.put(:sourceFormat, source_format)

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?

      assert changeset.errors == [
               {:topLevelSelector, {"is required", [validation: :required]}}
             ]

      where(source_format: ["xml", "text/xml"])
    end

    data_test "validates the schema appropriately when sourceType is #{source_type} and schema is #{inspect(schema)}" do
      changes = @valid_changes |> Map.put(:schema, schema) |> Map.put(:sourceType, source_type)

      changeset = DatasetInput.light_validation_changeset(changes)

      assert changeset.valid? == valid
      assert changeset.errors == errors

      where(
        source_type: ["ingest", "stream", "ingest", "something-else"],
        schema: [nil, nil, [], nil],
        errors: [
          [{:schema, {"is required", [validation: :required]}}],
          [{:schema, {"is required", [validation: :required]}}],
          [{:schema, {"cannot be empty", []}}],
          []
        ],
        valid: [false, false, false, true]
      )
    end

    test "xml source format requires all fields in the schema to have selectors" do
      schema = [
        %{name: "field_name"},
        %{name: "other_field", selector: "this is the only selector"},
        %{name: "another_field", selector: ""}
      ]

      changes =
        @valid_changes
        |> Map.merge(%{
          schema: schema,
          sourceFormat: "xml",
          topLevelSelector: "whatever",
          sourceType: "ingest"
        })

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert length(changeset.errors) == 2

      assert changeset.errors
             |> Enum.any?(fn {:schema, {error, _}} -> String.match?(error, ~r/selector.+field_name/) end)

      assert changeset.errors
             |> Enum.any?(fn {:schema, {error, _}} -> String.match?(error, ~r/selector.+another_field/) end)
    end

    data_test "is invalid when #{field} has an unacceptable value" do
      changes = @valid_changes |> Map.put(field, value)
      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert [{^field, {^message, _}}] = changeset.errors

      where([
        [:field, :value, :message],
        [:benefitRating, 0.7, "should be one of [0.0, 0.5, 1.0]"],
        [:benefitRating, 1.1, "should be one of [0.0, 0.5, 1.0]"],
        [:riskRating, 3.14159, "should be one of [0.0, 0.5, 1.0]"],
        [:riskRating, 0.000001, "should be one of [0.0, 0.5, 1.0]"]
      ])
    end

    data_test "#{field} are invalid when any key is not set" do
      changes =
        @valid_changes
        |> Map.put(field, [
          %{id: Ecto.UUID.generate(), key: "foo", value: "bar"},
          %{id: Ecto.UUID.generate(), key: "", value: "where's my key?"}
        ])

      changeset = DatasetInput.light_validation_changeset(changes)

      refute changeset.valid?
      assert changeset.errors == [{field, {"has invalid format", [validation: :format]}}]

      where(field: [:sourceQueryParams, :sourceHeaders])
    end

    data_test "#{field} are valid when they are not set" do
      changes = @valid_changes |> Map.delete(field)

      changeset = DatasetInput.light_validation_changeset(changes)

      assert changeset.valid?
      assert Enum.empty?(changeset.errors)

      where(field: [:sourceQueryParams, :sourceHeaders])
    end
  end

  describe "full_validation_changeset" do
    test "requires unique orgName and dataName" do
      changes = @valid_changes |> Map.delete(:id)

      existing_dataset = TDG.create_dataset(%{technical: %{dataName: @valid_changes.dataName, orgName: @valid_changes.orgName}})

      DatasetCache.put(existing_dataset)

      changeset = DatasetInput.full_validation_changeset(changes)

      refute changeset.valid?
      assert changeset.errors == [{:dataName, {"existing dataset has the same orgName and dataName", []}}]
    end

    test "allows same orgName and dataName when id is same" do
      existing_dataset =
        TDG.create_dataset(%{
          id: @valid_changes.id,
          technical: %{dataName: @valid_changes.dataName, orgName: @valid_changes.orgName}
        })

      DatasetCache.put(existing_dataset)

      changeset = DatasetInput.full_validation_changeset(@valid_changes)

      assert changeset.valid?
      assert changeset.errors == []
    end

    test "includes light validation" do
      changes = @valid_changes |> Map.put(:contactEmail, "nope") |> Map.delete(:sourceFormat)

      changeset = DatasetInput.full_validation_changeset(changes)

      refute changeset.valid?
      assert {:contactEmail, {"has invalid format", [validation: :format]}} in changeset.errors
      assert {:sourceFormat, {"is required", [validation: :required]}} in changeset.errors
    end
  end

  describe "add_key_value" do
    setup do
      %{changeset: DatasetInput.light_validation_changeset(@valid_changes)}
    end

    data_test "appends key/value to #{field}", %{changeset: changeset} do
      new_param = %{key: "key2", value: "value2"}

      changes =
        DatasetInput.add_key_value(changeset, field, new_param)
        |> Ecto.Changeset.apply_changes()

      assert length(changes[field]) == length(@valid_changes[field]) + 1
      refute is_nil(List.last(changes[field]).id)
      assert List.last(changes[field]).key == new_param.key
      assert List.last(changes[field]).value == new_param.value

      where(field: [:sourceQueryParams, :sourceHeaders])
    end

    data_test "appends an empty key/value to #{field} by default", %{changeset: changeset} do
      changes =
        DatasetInput.add_key_value(changeset, field)
        |> Ecto.Changeset.apply_changes()

      assert length(changes[field]) == length(@valid_changes[field]) + 1
      refute is_nil(List.last(changes[field]).id)
      assert is_nil(List.last(changes[field]).key)
      assert is_nil(List.last(changes[field]).value)

      where(field: [:sourceQueryParams, :sourceHeaders])
    end

    data_test "appends a key/value to an empty list of #{field}" do
      new_param = %{key: "key2", value: "value2"}

      changeset =
        @valid_changes
        |> Map.put(field, %{})
        |> DatasetInput.light_validation_changeset()

      changes =
        DatasetInput.add_key_value(changeset, field, new_param)
        |> Ecto.Changeset.apply_changes()

      assert length(changes[field]) == 1
      refute is_nil(hd(changes[field]).id)
      assert hd(changes[field]).key == new_param.key
      assert hd(changes[field]).value == new_param.value

      where(field: [:sourceQueryParams, :sourceHeaders])
    end
  end

  describe "remove_key_value" do
    setup do
      %{changeset: DatasetInput.light_validation_changeset(@valid_changes)}
    end

    data_test "removes key/value from #{field} by id", %{changeset: changeset} do
      changes =
        DatasetInput.remove_key_value(changeset, field, id)
        |> Ecto.Changeset.apply_changes()

      assert length(changes[field]) == length(@valid_changes[field]) - 1
      refute Enum.any?(changes[field], fn param -> param.id == id end)

      where(
        field: [:sourceQueryParams, :sourceHeaders],
        id: [@source_query_param_id, @source_header_id]
      )
    end

    data_test "does not alter #{field} if id is unknown", %{changeset: changeset} do
      assert changeset == DatasetInput.remove_key_value(changeset, field, "unknown")

      where(field: [:sourceQueryParams, :sourceHeaders])
    end

    data_test("removes error when an invalid key/value is removed from #{field}") do
      bad_key_value_id = Ecto.UUID.generate()

      changes =
        @valid_changes
        |> Map.put(field, [
          %{id: Ecto.UUID.generate(), key: "foo", value: "bar"},
          %{id: bad_key_value_id, key: "", value: "where's my key?"}
        ])

      changeset = DatasetInput.light_validation_changeset(changes)

      post_removal_changeset = DatasetInput.remove_key_value(changeset, field, bad_key_value_id)

      assert Enum.empty?(post_removal_changeset.errors)

      where(field: [:sourceQueryParams, :sourceHeaders])
    end

    data_test("updates source url when a sourceQueryParam is removed") do
      current_state =
        create_changeset(%{
          sourceUrl: "http://host",
          sourceQueryParams: sourceQueryParams
        })

      source_url =
        Enum.reduce(keysToRemove, current_state, fn key, acc ->
          DatasetInput.remove_key_value(acc, :sourceQueryParams, key)
        end)
        |> Ecto.Changeset.apply_changes()
        |> Map.get(:sourceUrl)

      assert source_url == sourceUrl

      where([
        [:sourceQueryParams, :sourceUrl, :keysToRemove],
        [[%{id: uuid("1"), key: "a", value: "b"}, %{id: uuid("2"), key: "c", value: "d"}], "http://host?c=d", [uuid("1")]],
        [[%{id: uuid("1"), key: "a", value: "b"}, %{id: uuid("2"), key: "c", value: "d"}], "http://host", [uuid("1"), uuid("2")]]
      ])
    end
  end

  defp uuid(known_value) do
    padded_known_value = Base.encode16(known_value, case: :lower) |> String.pad_leading(12, "0")

    "00000000-0000-0000-0000-#{padded_known_value}"
    |> Ecto.UUID.cast!()
  end

  describe "adjust_source_query_params_for_url/1" do
    test "given a url it sets the query params to match what is in it" do
      current_state = create_changeset(%{sourceUrl: "https://source.url.example.com?look=at&me=i&have=params"})

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_query_params_for_url()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceQueryParams: [
                 %{key: "look", value: "at"},
                 %{key: "me", value: "i"},
                 %{key: "have", value: "params"}
               ]
             } = dataset_input
    end

    test "given a url and a changeset with non-empty query params it replaces the query params to match what is in the url" do
      current_state = create_changeset(%{sourceUrl: "https://source.url.example.com?look=at&me=i&have=params"})

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_query_params_for_url()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceQueryParams: [
                 %{key: "look", value: "at"},
                 %{key: "me", value: "i"},
                 %{key: "have", value: "params"}
               ]
             } = dataset_input
    end

    test "given a url with partial URL encoding characters in it ignores them" do
      current_state =
        create_changeset(%{
          sourceUrl: "https://source.url.example.com?invalid%=stuff",
          sourceQueryParams: [%{id: "uuid-1", key: "still", value: "here"}]
        })

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_query_params_for_url()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceQueryParams: [
                 %{key: "still", value: "here"}
               ]
             } = dataset_input
    end

    test "given a url with URL encoding characters in it already it matches what's in the url" do
      current_state = create_changeset(%{sourceUrl: "https://source.url.example.com?hello%20world=true&goodbye+scott=false"})

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_query_params_for_url()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceQueryParams: [
                 %{key: "hello world", value: "true"},
                 %{key: "goodbye scott", value: "false"}
               ]
             } = dataset_input
    end
  end

  describe "adjust_source_url_for_query_params/1" do
    test "given a url (with no params) and query params it sets url to match the query params" do
      current_state =
        create_changeset(%{
          sourceUrl: "https://source.url.example.com",
          sourceQueryParams: [
            %{id: "uuid-1", key: "look", value: "at"},
            %{id: "uuid-2", key: "me", value: "i"},
            %{id: "uuid-3", key: "have", value: "params"}
          ]
        })

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_url_for_query_params()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceUrl: "https://source.url.example.com?look=at&me=i&have=params",
               sourceQueryParams: [
                 %{key: "look", value: "at"},
                 %{key: "me", value: "i"},
                 %{key: "have", value: "params"}
               ]
             } = dataset_input
    end

    test "given a url and query params it sets url to match the query params" do
      current_state =
        create_changeset(%{
          sourceUrl: "https://source.url.example.com?somehow=existing&params=yes",
          sourceQueryParams: [
            %{id: "uuid-1", key: "look", value: "at"},
            %{id: "uuid-2", key: "me", value: "i"},
            %{id: "uuid-3", key: "have", value: "params"}
          ]
        })

      dataset_input =
        current_state
        |> DatasetInput.adjust_source_url_for_query_params()
        |> Ecto.Changeset.apply_changes()

      assert %{
               sourceUrl: "https://source.url.example.com?look=at&me=i&have=params",
               sourceQueryParams: [
                 %{key: "look", value: "at"},
                 %{key: "me", value: "i"},
                 %{key: "have", value: "params"}
               ]
             } = dataset_input
    end

    test "given a url with at least one invalid query param it marks the dataset as invalid" do
      current_state = create_changeset(%{sourceUrl: "https://source.url.example.com?=oops&a=b"})

      changeset = DatasetInput.adjust_source_query_params_for_url(current_state)

      refute changeset.valid?
      assert changeset.errors == [{:sourceQueryParams, {"has invalid format", [validation: :format]}}]

      assert %{sourceQueryParams: [%{key: nil, value: "oops"}, %{key: "a", value: "b"}]} = Ecto.Changeset.apply_changes(changeset)
    end
  end

  defp create_changeset(overrides) do
    @valid_changes
    |> Map.merge(overrides)
    |> DatasetInput.light_validation_changeset()
  end
end
