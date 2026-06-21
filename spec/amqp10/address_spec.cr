require "spec"
require "../../src/lavinmq/amqp10/address"

include LavinMQ::AMQP10

describe LavinMQ::AMQP10::Address do
  describe ".parse_target" do
    it "parses /exchanges/:exchange/:routing-key" do
      t = Address.parse_target("/exchanges/amq.direct/rk").not_nil!
      t.exchange.should eq "amq.direct"
      t.routing_key.should eq "rk"
    end

    it "parses /exchanges/:exchange with empty routing key" do
      t = Address.parse_target("/exchanges/amq.fanout").not_nil!
      t.exchange.should eq "amq.fanout"
      t.routing_key.should eq ""
    end

    it "parses /queues/:queue to the default exchange" do
      t = Address.parse_target("/queues/q1").not_nil!
      t.exchange.should eq ""
      t.routing_key.should eq "q1"
    end

    it "percent-decodes names per RFC 3986" do
      t = Address.parse_target("/exchanges/amq.direct/my-routing_key%2F123").not_nil!
      t.routing_key.should eq "my-routing_key/123"
    end

    it "returns nil for an anonymous (null/empty) target" do
      Address.parse_target(nil).should be_nil
      Address.parse_target("").should be_nil
    end

    it "raises on an unsupported target" do
      expect_raises(LavinMQ::AMQP10::Error) { Address.parse_target("/topics/foo") }
    end
  end

  describe ".parse_source" do
    it "parses /queues/:queue" do
      Address.parse_source("/queues/q1").should eq "q1"
    end

    it "percent-decodes the queue name" do
      Address.parse_source("/queues/my%20queue").should eq "my queue"
    end

    it "raises on a non-queue source" do
      expect_raises(LavinMQ::AMQP10::Error) { Address.parse_source("/exchanges/amq.direct") }
    end
  end
end
