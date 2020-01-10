defmodule Pipeline.Writer.TableWriter.StatementCreateTest do
  use ExUnit.Case
  use Placebo

  alias Pipeline.Writer.TableWriter.Statement

  describe "create_schema/1" do
    @tag capture_log: true
    test "creates schema if not exist" do
      expected = ~s|CREATE SCHEMA IF NOT EXISTS schema_name|

      assert {:ok, ^expected} = Statement.create_schema(%{schema_name: "schema_name"})
    end
  end

  describe "create/1" do
    @tag capture_log: true
    test "creates table in given schema" do
      schema = [
        %{name: "first_name", type: "string"}
      ]

      schema_name = "special_schema"

      expected = ~s|CREATE TABLE IF NOT EXISTS special_schema.table_name ("first_name" varchar)|

      assert {:ok, ^expected} = Statement.create(%{table: "table_name", schema: schema, schema_name: schema_name})
    end

    @tag capture_log: true
    test "converts schema type value to proper presto type" do
      schema = [
        %{name: "first_name", type: "string"},
        %{name: "height", type: "long"},
        %{name: "weight", type: "float"},
        %{name: "identifier", type: "decimal"},
        %{name: "payload", type: "json"}
      ]

      expected =
        ~s|CREATE TABLE IF NOT EXISTS default.table_name ("first_name" varchar, "height" bigint, "weight" double, "identifier" decimal, "payload" varchar)|

      assert {:ok, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    @tag capture_log: true
    test "handles row" do
      schema = [
        %{
          name: "spouse",
          type: "map",
          subSchema: [
            %{name: "first_name", type: "string"},
            %{
              name: "next_of_kin",
              type: "map",
              subSchema: [
                %{name: "first_name", type: "string"},
                %{name: "date_of_birth", type: "date"}
              ]
            }
          ]
        }
      ]

      expected =
        ~s|CREATE TABLE IF NOT EXISTS default.table_name ("spouse" row("first_name" varchar, "next_of_kin" row("first_name" varchar, "date_of_birth" date)))|

      assert {:ok, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    @tag capture_log: true
    test "handles array" do
      schema = [
        %{name: "friend_names", type: "list", itemType: "string"}
      ]

      expected = ~s|CREATE TABLE IF NOT EXISTS default.table_name ("friend_names" array(varchar))|
      assert {:ok, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    @tag capture_log: true
    test "handles array of maps" do
      schema = [
        %{
          name: "friend_groups",
          type: "list",
          itemType: "map",
          subSchema: [
            %{name: "first_name", type: "string"},
            %{name: "last_name", type: "string"}
          ]
        }
      ]

      expected =
        ~s|CREATE TABLE IF NOT EXISTS default.table_name ("friend_groups" array(row("first_name" varchar, "last_name" varchar)))|

      assert {:ok, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    @tag capture_log: true
    test "returns error tuple with type message when field cannot be mapped" do
      schema = [%{name: "my_field", type: "unsupported"}]
      expected = "unsupported Type is not supported"
      assert {:error, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    @tag capture_log: true
    test "returns error tuple when given invalid schema" do
      schema = [%{name: "my_field"}]
      expected = "Unable to parse schema: %KeyError{key: :type, message: nil, term: %{name: \"my_field\"}}"
      assert {:error, ^expected} = Statement.create(%{table: "table_name", schema: schema})
    end

    test "accepts a select statement to create table from" do
      expected = "create table default.one__two as (select * from three__four)"
      assert {:ok, ^expected} = Statement.create(%{table: "one__two", as: "select * from three__four"})
    end

    test "select statement can be in given schema" do
      schema_name = "special_schema"
      expected = "create table #{schema_name}.one__two as (select * from three__four)"

      assert {:ok, ^expected} =
               Statement.create(%{table: "one__two", as: "select * from three__four", schema_name: schema_name})
    end
  end
end
