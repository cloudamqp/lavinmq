require "./spec_helper"
require "../src/lavinmq/replay/stamp.cr"

private def build_msg(headers : Hash(String, AMQ::Protocol::Field) = Hash(String, AMQ::Protocol::Field).new,
                      exchange : String = "in-ex",
                      routing_key : String = "in-rk",
                      content_type : String? = nil,
                      delivery_mode : UInt8? = nil) : LavinMQ::Message
  props = AMQ::Protocol::Properties.new(content_type: content_type, delivery_mode: delivery_mode)
  props.headers = AMQ::Protocol::Table.new(headers) unless headers.empty?
  LavinMQ::Message.new(exchange, routing_key, "payload", props)
end

describe LavinMQ::Replay do
  describe ".stamp_intake" do
    it "stamps x-replay-id when missing" do
      msg = build_msg({"x-source-queue" => "src".as(AMQ::Protocol::Field)})
      stamped = LavinMQ::Replay.stamp_intake(msg)
      id = stamped.headers.not_nil!["x-replay-id"]?.try(&.to_s)
      id.should_not be_nil
      id.not_nil!.size.should be > 10
    end

    it "preserves an existing x-replay-id" do
      msg = build_msg({
        "x-source-queue" => "src".as(AMQ::Protocol::Field),
        "x-replay-id"    => "already-set".as(AMQ::Protocol::Field),
      })
      stamped = LavinMQ::Replay.stamp_intake(msg)
      stamped.headers.not_nil!["x-replay-id"].should eq "already-set"
    end

    it "derives x-source-* from x-first-death-* when missing" do
      msg = build_msg({
        "x-first-death-queue"       => "orig-q".as(AMQ::Protocol::Field),
        "x-first-death-exchange"    => "orig-ex".as(AMQ::Protocol::Field),
        "x-first-death-routing-key" => "orig-rk".as(AMQ::Protocol::Field),
      })
      stamped = LavinMQ::Replay.stamp_intake(msg)
      h = stamped.headers.not_nil!
      h["x-source-queue"].should eq "orig-q"
      h["x-source-exchange"].should eq "orig-ex"
      h["x-source-routing-key"].should eq "orig-rk"
    end

    it "falls back to msg.exchange_name when x-first-death-exchange is absent" do
      msg = build_msg(
        {"x-first-death-queue" => "orig-q".as(AMQ::Protocol::Field)},
        exchange: "in-ex",
        routing_key: "in-rk",
      )
      stamped = LavinMQ::Replay.stamp_intake(msg)
      h = stamped.headers.not_nil!
      h["x-source-exchange"].should eq "in-ex"
      h["x-source-routing-key"].should eq "in-rk"
    end

    it "falls back to msg.routing_key when x-first-death-routing-key is absent" do
      msg = build_msg(
        {
          "x-first-death-queue"    => "orig-q".as(AMQ::Protocol::Field),
          "x-first-death-exchange" => "dlx".as(AMQ::Protocol::Field),
        },
        exchange: "dlx",
        routing_key: "orig-rk",
      )
      stamped = LavinMQ::Replay.stamp_intake(msg)
      stamped.headers.not_nil!["x-source-routing-key"].should eq "orig-rk"
    end

    it "does not overwrite existing x-source-* headers" do
      msg = build_msg({
        "x-source-queue"      => "explicit-q".as(AMQ::Protocol::Field),
        "x-first-death-queue" => "death-q".as(AMQ::Protocol::Field),
      })
      stamped = LavinMQ::Replay.stamp_intake(msg)
      stamped.headers.not_nil!["x-source-queue"].should eq "explicit-q"
    end

    it "stamps x-source-timestamp if missing" do
      msg = build_msg({"x-source-queue" => "src".as(AMQ::Protocol::Field)})
      stamped = LavinMQ::Replay.stamp_intake(msg)
      ts = stamped.headers.not_nil!["x-source-timestamp"]?
      ts.should_not be_nil
      ts.as(Int64).should be > 0_i64
    end

    it "raises when neither x-source-queue nor x-first-death-queue is present" do
      msg = build_msg({"unrelated" => "value".as(AMQ::Protocol::Field)})
      expect_raises(LavinMQ::Error::PreconditionFailed, /cannot enter replay queue/) do
        LavinMQ::Replay.stamp_intake(msg)
      end
    end

    it "raises when headers are nil" do
      msg = build_msg
      expect_raises(LavinMQ::Error::PreconditionFailed) do
        LavinMQ::Replay.stamp_intake(msg)
      end
    end

    it "preserves other AMQP property fields" do
      msg = build_msg(
        {"x-source-queue" => "src".as(AMQ::Protocol::Field)},
        content_type: "application/json",
        delivery_mode: 2_u8,
      )
      stamped = LavinMQ::Replay.stamp_intake(msg)
      stamped.content_type.should eq "application/json"
      stamped.delivery_mode.should eq 2_u8
    end

    it "does not mutate the source Properties' headers" do
      msg = build_msg({"x-source-queue" => "src".as(AMQ::Protocol::Field)})
      LavinMQ::Replay.stamp_intake(msg)
      msg.properties.headers.not_nil!.has_key?("x-replay-id").should be_false
    end
  end
end
