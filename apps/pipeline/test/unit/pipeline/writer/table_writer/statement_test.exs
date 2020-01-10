defmodule Pipeline.Writer.TableWriter.StatementTest do
  use ExUnit.Case
  use Placebo

  alias Pipeline.Writer.TableWriter.Statement

  describe "drop/1" do
    test "generates a valid DROP TABLE statement" do
      expected = "drop table if exists foo__bar"
      assert ^expected = Statement.drop(%{table: "foo__bar"})
    end
  end

  describe "alter/1" do
    test "generates a valid ALTER TABLE statement" do
      expected = "alter table foo__bar rename to foo__baz"
      assert ^expected = Statement.alter(%{table: "foo__bar", alteration: "rename to foo__baz"})
    end
  end
end
