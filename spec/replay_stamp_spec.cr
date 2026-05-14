require "./spec_helper"
require "../src/lavinmq/replay/stamp.cr"

describe LavinMQ::Replay do
  describe ".stamp_intake" do
    it "stamps x-replay-id when missing" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({"x-source-queue" => "src"})
      stamped = LavinMQ::Replay.stamp_intake(props)
      id = stamped.headers.not_nil!["x-replay-id"]?.try(&.to_s)
      id.should_not be_nil
      id.not_nil!.size.should be > 10
    end

    it "preserves an existing x-replay-id" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({
        "x-source-queue" => "src",
        "x-replay-id"    => "already-set",
      })
      stamped = LavinMQ::Replay.stamp_intake(props)
      stamped.headers.not_nil!["x-replay-id"].should eq "already-set"
    end

    it "derives x-source-* from x-first-death-* when missing" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({
        "x-first-death-queue"       => "orig-q",
        "x-first-death-exchange"    => "orig-ex",
        "x-first-death-routing-key" => "orig-rk",
      })
      stamped = LavinMQ::Replay.stamp_intake(props)
      h = stamped.headers.not_nil!
      h["x-source-queue"].should eq "orig-q"
      h["x-source-exchange"].should eq "orig-ex"
      h["x-source-routing-key"].should eq "orig-rk"
    end

    it "does not overwrite existing x-source-* headers" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({
        "x-source-queue"      => "explicit-q",
        "x-first-death-queue" => "death-q",
      })
      stamped = LavinMQ::Replay.stamp_intake(props)
      stamped.headers.not_nil!["x-source-queue"].should eq "explicit-q"
    end

    it "stamps x-source-timestamp if missing" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({"x-source-queue" => "src"})
      stamped = LavinMQ::Replay.stamp_intake(props)
      ts = stamped.headers.not_nil!["x-source-timestamp"]?
      ts.should_not be_nil
      ts.as(Int64).should be > 0_i64
    end

    it "raises when neither x-source-queue nor x-first-death-queue is present" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({"unrelated" => "value"})
      expect_raises(LavinMQ::Error::PreconditionFailed, /cannot enter replay queue/) do
        LavinMQ::Replay.stamp_intake(props)
      end
    end

    it "raises when headers are nil" do
      props = AMQ::Protocol::Properties.new
      expect_raises(LavinMQ::Error::PreconditionFailed) do
        LavinMQ::Replay.stamp_intake(props)
      end
    end

    it "preserves other AMQP property fields" do
      props = AMQ::Protocol::Properties.new(
        content_type: "application/json",
        delivery_mode: 2_u8,
        priority: 5_u8,
        message_id: "m1",
      )
      props.headers = AMQ::Protocol::Table.new({"x-source-queue" => "src"})
      stamped = LavinMQ::Replay.stamp_intake(props)
      stamped.content_type.should eq "application/json"
      stamped.delivery_mode.should eq 2_u8
      stamped.priority.should eq 5_u8
      stamped.message_id.should eq "m1"
    end

    it "does not mutate the source Properties' headers" do
      props = AMQ::Protocol::Properties.new
      props.headers = AMQ::Protocol::Table.new({"x-source-queue" => "src"})
      LavinMQ::Replay.stamp_intake(props)
      props.headers.not_nil!.has_key?("x-replay-id").should be_false
    end
  end
end
