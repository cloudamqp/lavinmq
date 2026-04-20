require "spec"
require "json"
require "../src/lavinmq_wasm/router"

describe "LavinMQWasm" do
  describe ".route_topic" do
    it "matches exact segment" do
      bindings = [{"q1", "logs.error"}]
      LavinMQWasm.route_topic(bindings, "logs.error").should eq ["q1"]
    end

    it "# matches zero or more segments" do
      bindings = [{"q1", "logs.#"}]
      LavinMQWasm.route_topic(bindings, "logs.error.critical").should eq ["q1"]
      LavinMQWasm.route_topic(bindings, "logs").should eq ["q1"]
    end

    it "* matches exactly one segment" do
      bindings = [{"q1", "logs.*"}]
      LavinMQWasm.route_topic(bindings, "logs.error").should eq ["q1"]
      LavinMQWasm.route_topic(bindings, "logs.error.critical").should eq [] of String
    end

    it "routes to multiple queues" do
      bindings = [{"all", "logs.#"}, {"errors", "logs.error"}]
      result = LavinMQWasm.route_topic(bindings, "logs.error")
      result.should contain "all"
      result.should contain "errors"
    end

    it "returns empty when no match" do
      bindings = [{"q1", "orders.#"}]
      LavinMQWasm.route_topic(bindings, "logs.error").should eq [] of String
    end
  end

  describe ".route_direct" do
    it "matches exact routing key" do
      bindings = [{"q1", "payments"}, {"q2", "orders"}]
      LavinMQWasm.route_direct(bindings, "payments").should eq ["q1"]
    end

    it "returns empty when no match" do
      bindings = [{"q1", "payments"}]
      LavinMQWasm.route_direct(bindings, "orders").should eq [] of String
    end
  end

  describe ".route_fanout" do
    it "matches all bindings" do
      bindings = [{"q1", ""}, {"q2", ""}, {"q3", ""}]
      LavinMQWasm.route_fanout(bindings).should eq ["q1", "q2", "q3"]
    end
  end

  describe ".route_headers" do
    it "matches all headers (x-match: all)" do
      bindings = [{
        "q1",
        {"format" => JSON::Any.new("pdf"), "type" => JSON::Any.new("report")},
      }]
      headers = {"format" => JSON::Any.new("pdf"), "type" => JSON::Any.new("report")}
      LavinMQWasm.route_headers(bindings, headers, "all").should eq ["q1"]
    end

    it "does not match if one header missing (x-match: all)" do
      bindings = [{"q1", {"format" => JSON::Any.new("pdf"), "type" => JSON::Any.new("report")}}]
      headers = {"format" => JSON::Any.new("pdf")}
      LavinMQWasm.route_headers(bindings, headers, "all").should eq [] of String
    end

    it "matches any header (x-match: any)" do
      bindings = [{"q1", {"x-match" => JSON::Any.new("any"), "format" => JSON::Any.new("pdf"), "type" => JSON::Any.new("report")}}]
      headers = {"format" => JSON::Any.new("pdf")}
      LavinMQWasm.route_headers(bindings, headers, "any").should eq ["q1"]
    end
  end

  describe ".route_consistent_hash" do
    it "routes to exactly one queue" do
      bindings = [{"q1", "1"}, {"q2", "1"}, {"q3", "1"}]
      result = LavinMQWasm.route_consistent_hash(bindings, "order-123", Hash(String, JSON::Any).new, Hash(String, JSON::Any).new)
      result.size.should eq 1
      (["q1", "q2", "q3"]).should contain result.first
    end

    it "consistently routes the same key to the same queue" do
      bindings = [{"q1", "1"}, {"q2", "1"}, {"q3", "1"}]
      headers = Hash(String, JSON::Any).new
      args = Hash(String, JSON::Any).new
      first = LavinMQWasm.route_consistent_hash(bindings, "my-key", headers, args)
      10.times do
        LavinMQWasm.route_consistent_hash(bindings, "my-key", headers, args).should eq first
      end
    end

    it "distributes different keys across queues" do
      bindings = [{"q1", "10"}, {"q2", "10"}, {"q3", "10"}]
      headers = Hash(String, JSON::Any).new
      args = Hash(String, JSON::Any).new
      results = (1..50).map { |i| LavinMQWasm.route_consistent_hash(bindings, "key-#{i}", headers, args).first? }.compact
      results.uniq.size.should be > 1
    end

    it "routes by header when x-hash-on is set" do
      bindings = [{"q1", "1"}, {"q2", "1"}]
      args = {"x-hash-on" => JSON::Any.new("user-id")}
      headers_a = {"user-id" => JSON::Any.new("alice")}
      headers_b = {"user-id" => JSON::Any.new("bob")}
      result_a = LavinMQWasm.route_consistent_hash(bindings, "ignored", headers_a, args)
      result_b = LavinMQWasm.route_consistent_hash(bindings, "ignored", headers_b, args)
      # same user always lands on same queue
      LavinMQWasm.route_consistent_hash(bindings, "ignored", headers_a, args).should eq result_a
      LavinMQWasm.route_consistent_hash(bindings, "ignored", headers_b, args).should eq result_b
    end

    it "uses jump algorithm when specified" do
      bindings = [{"q1", "1"}, {"q2", "1"}, {"q3", "1"}]
      args = {"x-algorithm" => JSON::Any.new("jump")}
      result = LavinMQWasm.route_consistent_hash(bindings, "test-key", Hash(String, JSON::Any).new, args)
      result.size.should eq 1
    end

    it "returns empty when no bindings" do
      LavinMQWasm.route_consistent_hash([] of {String, String}, "key", Hash(String, JSON::Any).new, Hash(String, JSON::Any).new).should eq [] of String
    end
  end

  describe "policy matching" do
    it "matches by regex pattern" do
      policy = LavinMQWasm::Policy.from_json({
        name: "ha", pattern: "^ha\\.", apply_to: "queues",
        priority: 0, definition: {} of String => JSON::Any,
      }.to_json)
      policy.matches?("ha.reports", "queue").should be_true
      policy.matches?("other", "queue").should be_false
    end

    it "matches [.] character class as literal dot" do
      policy = LavinMQWasm::Policy.from_json({
        name: "ttl", pattern: "^ha[.]", apply_to: "all",
        priority: 5, definition: {} of String => JSON::Any,
      }.to_json)
      policy.matches?("ha.billing", "queue").should be_true
      policy.matches?("haxbilling", "queue").should be_false
    end

    it "selects highest priority policy" do
      low = LavinMQWasm::Policy.from_json({name: "low", pattern: ".*", apply_to: "all", priority: 0, definition: {"max-length" => 1000}}.to_json)
      high = LavinMQWasm::Policy.from_json({name: "high", pattern: ".*", apply_to: "all", priority: 10, definition: {"max-length" => 100}}.to_json)
      matched, _ = LavinMQWasm.match_policies([low, high], "my-queue", "queue")
      matched.try(&.name).should eq "high"
    end
  end
end
