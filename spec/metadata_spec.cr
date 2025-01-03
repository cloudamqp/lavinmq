require "./spec_helper"

module LavinMQ
  struct NamedTupleMetadata(T) < Metadata
    describe Value do
      describe "compare" do
        {% begin %}
          {%
            testdata = {
              {1, 2, -1},
              {2, 1, 1},
              {"a", "b", -1},
              {"b", "a", 1},
              {1, 1, 0},
              {1, nil, 1},
              {nil, 1, -1},
              {nil, nil, 0},
              {1, "a", -1},
              {"a", 1, 1},
              {1u8, 2i32, -1},
            }
          %}
          {% for values in testdata %}
            {% left, right, expected = values %}
            it "{{left.id}} <=> {{right.id}} is {{expected}}" do
              res = Value.new({{left}}) <=> Value.new({{right}})
              res.should eq {{expected}}
            end
          {% end %}
        {% end %}
      end
    end
  end

  describe Metadata do
    data = Metadata.new({
      foo: "bar",
      baz: 1,
      sub: {
        bar:  "foo",
        next: {
          value: 2,
        },
      },
    })

    describe "#dig" do
      it "can return first level value" do
        data.dig("foo").should eq Metadata::Value.new("bar")
      end

      it "can return value from deep level" do
        data.dig("sub.next.value").should eq Metadata::Value.new(2)
      end

      it "raises on invalid path" do
        expect_raises(KeyError) do
          data.dig("invalid.path")
        end
      end
    end

    describe "#dig?" do
      it "returns Value(Nil) for invalid path" do
        data.dig?("invalid.path").should be_a Metadata::Value(Nil)
      end
    end

    describe "#[]" do
      it "returns expected value" do
        data.dig("foo").should eq Metadata::Value.new("bar")
      end

      it "raises on invalid key" do
        expect_raises(KeyError) do
          data.dig("invalid")
        end
      end
    end

    describe "#[]?" do
      it "raises on invalid key" do
        data["invalid"]?.should be_a Metadata::Value(Nil)
      end
    end
  end
end
