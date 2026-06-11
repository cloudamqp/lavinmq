require "spec"
require "../src/lavinmq/amqp"

# Specs for the vendored AMQ::Protocol::Table#has_entry? extension
# (src/lavinmq/amqp/table_ext.cr). Mirrors the upstream amq-protocol specs.
describe AMQ::Protocol::Table do
  describe "#has_entry?" do
    it "returns true when key is present and value is equal" do
      t = AMQ::Protocol::Table.new({str: "foo", int: 42, bool: true, nilval: nil, float: 1.5})
      t.has_entry?("str", "foo").should be_true
      t.has_entry?("int", 42).should be_true
      t.has_entry?("bool", true).should be_true
      t.has_entry?("nilval", nil).should be_true
      t.has_entry?("float", 1.5).should be_true
    end

    it "returns false when key is present but value differs" do
      t = AMQ::Protocol::Table.new({str: "foo", int: 42, bool: true})
      t.has_entry?("str", "bar").should be_false
      t.has_entry?("str", "fo").should be_false
      t.has_entry?("int", 43).should be_false
      t.has_entry?("bool", false).should be_false
    end

    it "returns false when key is absent" do
      t = AMQ::Protocol::Table.new({a: 1})
      t.has_entry?("b", 1).should be_false
      AMQ::Protocol::Table.new.has_entry?("a", 1).should be_false
    end

    it "compares numerics across types" do
      t = AMQ::Protocol::Table.new({a: 1_i64, b: 1_u8})
      t.has_entry?("a", 1_i32).should be_true
      t.has_entry?("b", 1_i64).should be_true
      t.has_entry?("a", 2_i32).should be_false
    end

    it "returns false on type mismatch" do
      t = AMQ::Protocol::Table.new({str: "1", bytes: "foo".to_slice, int: 1})
      t.has_entry?("str", 1).should be_false
      t.has_entry?("bytes", "foo").should be_false
      t.has_entry?("int", "1").should be_false
    end

    it "compares nested table values" do
      t = AMQ::Protocol::Table.new({nested: {"a" => 1}})
      t.has_entry?("nested", AMQ::Protocol::Table.new({a: 1})).should be_true
      t.has_entry?("nested", AMQ::Protocol::Table.new({a: 2})).should be_false
    end

    it "behaves like has_key? && fetch == value" do
      t = AMQ::Protocol::Table.new({str: "foo", int: 42, nilval: nil})
      {"str", "int", "nilval", "missing"}.each do |key|
        {"foo", 42, nil, "other"}.each do |value|
          expected = t.has_key?(key) && t[key] == value
          t.has_entry?(key, value).should eq(expected)
        end
      end
    end
  end
end
