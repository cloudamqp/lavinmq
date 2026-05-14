require "./spec_helper"

describe LavinMQ::AMQP::ReplayQueue do
  it "is declared via x-queue-type: replay and is durable" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("replay-1", durable: true,
          args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        q = s.vhosts["/"].queue?("replay-1")
        q.should_not be_nil
        q.should be_a(LavinMQ::AMQP::ReplayQueue)
        q.not_nil!.durable?.should be_true
      end
    end
  end

  it "rejects non-durable declaration" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, /non-durable/) do
          ch.queue("replay-bad", durable: false,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        end
      end
    end
  end

  it "accepts a message that already carries x-source-queue and stamps x-replay-id" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("replay-accept", durable: true,
          args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
          "x-source-queue"       => "src",
          "x-source-exchange"    => "",
          "x-source-routing-key" => "src",
        }))
        ch.basic_publish_confirm("hello", "", "replay-accept", props: props)
        sleep 20.milliseconds
        ch.queue_declare("replay-accept", passive: true)[:message_count].should eq 1
        msg = ch.basic_get("replay-accept", no_ack: true).not_nil!
        headers = msg.properties.headers.not_nil!
        headers.has_key?("x-replay-id").should be_true
        headers["x-source-queue"].should eq "src"
      end
    end
  end

  it "derives x-source-* from x-first-death-* when only those are present" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("replay-death", durable: true,
          args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
          "x-first-death-queue"       => "orig",
          "x-first-death-exchange"    => "amq.direct",
          "x-first-death-routing-key" => "rk",
        }))
        ch.basic_publish_confirm("dl", "", "replay-death", props: props)
        sleep 20.milliseconds
        msg = ch.basic_get("replay-death", no_ack: true).not_nil!
        h = msg.properties.headers.not_nil!
        h["x-source-queue"].should eq "orig"
        h["x-source-exchange"].should eq "amq.direct"
        h["x-source-routing-key"].should eq "rk"
      end
    end
  end

  it "refuses a message that lacks both x-source-queue and x-first-death-queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("replay-refuse", durable: true,
          args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        ch.basic_publish_confirm("nope", "", "replay-refuse")
        sleep 20.milliseconds
        ch.queue_declare("replay-refuse", passive: true)[:message_count].should eq 0
      end
    end
  end

  it "stamps a fresh x-replay-id per message" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue("replay-ids", durable: true,
          args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
        props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
          "x-source-queue"       => "src",
          "x-source-exchange"    => "",
          "x-source-routing-key" => "src",
        }))
        2.times { ch.basic_publish_confirm("m", "", "replay-ids", props: props) }
        sleep 20.milliseconds
        ids = [] of String
        2.times do
          msg = ch.basic_get("replay-ids", no_ack: true).not_nil!
          ids << msg.properties.headers.not_nil!["x-replay-id"].to_s
        end
        ids.uniq.size.should eq 2
      end
    end
  end
end
