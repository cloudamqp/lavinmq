require "./spec_helper"

describe "poison-pill diversion" do
  describe "x-quarantine-after-redeliveries" do
    it "diverts the message to the target after threshold is exceeded" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          target = ch.queue("pp-target-1")
          source = ch.queue("pp-source-1", durable: true, args: AMQP::Client::Arguments.new({
            "x-quarantine-after-redeliveries" => 2,
            "x-quarantine-target"             => "pp-target-1",
          }))
          source.publish_confirm("payload", props: AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({"user-key" => "v"})))

          3.times do
            msg = wait_for { source.get(no_ack: false) }.not_nil!
            msg.nack(requeue: true)
          end

          diverted = wait_for { target.get(no_ack: true) }.not_nil!
          diverted.body_io.to_s.should eq "payload"
          headers = diverted.properties.headers.not_nil!
          headers["x-source-queue"].should eq "pp-source-1"
          headers["x-source-exchange"].should eq ""
          headers["x-source-routing-key"].should eq "pp-source-1"
          headers["x-source-timestamp"].as(Int).should be > 0
          headers["x-delivery-count"].as(Int).should be > 0
          headers["user-key"].should eq "v"

          s.vhosts["/"].queue?("pp-source-1").not_nil!.message_count.should eq 0
        end
      end
    end

    it "preserves backwards-compatibility: falls back to DLX when no target is set" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("pp-dlx-1")
          source = ch.queue("pp-source-2", durable: true, args: AMQP::Client::Arguments.new({
            "x-quarantine-after-redeliveries" => 1,
            "x-dead-letter-exchange"          => "",
            "x-dead-letter-routing-key"       => "pp-dlx-1",
          }))
          source.publish_confirm("payload")

          2.times do
            msg = wait_for { source.get(no_ack: false) }.not_nil!
            msg.nack(requeue: true)
          end

          dl_msg = wait_for { dlq.get(no_ack: true) }.not_nil!
          dl_msg.body_io.to_s.should eq "payload"
        end
      end
    end
  end

  describe "x-nack-to-quarantine" do
    it "diverts a nacked (requeue=false) message to the target with x-source-* headers" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          target = ch.queue("pp-target-3")
          source = ch.queue("pp-source-3", durable: true, args: AMQP::Client::Arguments.new({
            "x-nack-to-quarantine" => true,
            "x-quarantine-target"  => "pp-target-3",
          }))
          source.publish_confirm("payload")

          msg = wait_for { source.get(no_ack: false) }.not_nil!
          msg.nack(requeue: false)

          diverted = wait_for { target.get(no_ack: true) }.not_nil!
          diverted.body_io.to_s.should eq "payload"
          headers = diverted.properties.headers.not_nil!
          headers["x-source-queue"].should eq "pp-source-3"
          s.vhosts["/"].queue?("pp-source-3").not_nil!.message_count.should eq 0
        end
      end
    end
  end

  describe "x-quarantine-action :drop" do
    it "silently deletes the message and skips DLX" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          dlq = ch.queue("pp-dlx-4")
          source = ch.queue("pp-source-4", durable: true, args: AMQP::Client::Arguments.new({
            "x-nack-to-quarantine"      => true,
            "x-quarantine-action"       => "drop",
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "pp-dlx-4",
          }))
          source.publish_confirm("payload")

          msg = wait_for { source.get(no_ack: false) }.not_nil!
          msg.nack(requeue: false)

          sleep 50.milliseconds
          s.vhosts["/"].queue?("pp-source-4").not_nil!.message_count.should eq 0
          dlq.message_count.should eq 0
        end
      end
    end
  end

  describe "x-quarantine-action :tee" do
    it "copies to target AND dead-letters the original" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          target = ch.queue("pp-target-5")
          dlq = ch.queue("pp-dlx-5")
          source = ch.queue("pp-source-5", durable: true, args: AMQP::Client::Arguments.new({
            "x-nack-to-quarantine"      => true,
            "x-quarantine-action"       => "tee",
            "x-quarantine-target"       => "pp-target-5",
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "pp-dlx-5",
          }))
          source.publish_confirm("payload")

          msg = wait_for { source.get(no_ack: false) }.not_nil!
          msg.nack(requeue: false)

          copy = wait_for { target.get(no_ack: true) }.not_nil!
          copy.body_io.to_s.should eq "payload"
          copy.properties.headers.not_nil!["x-source-queue"].should eq "pp-source-5"

          dl_msg = wait_for { dlq.get(no_ack: true) }.not_nil!
          dl_msg.body_io.to_s.should eq "payload"

          s.vhosts["/"].queue?("pp-source-5").not_nil!.message_count.should eq 0
        end
      end
    end
  end
end
