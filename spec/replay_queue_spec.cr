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

  describe "scan helpers" do
    private_props = AMQP::Client::Properties.new(headers: AMQP::Client::Arguments.new({
      "x-source-queue"       => "src",
      "x-source-exchange"    => "",
      "x-source-routing-key" => "src",
    }))

    it "find_envelope_with_header locates a message by x-replay-id" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.queue("replay-scan", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          3.times do |i|
            ch.basic_publish_confirm("m-#{i}", "", "replay-scan", props: private_props)
          end
          q = s.vhosts["/"].queue?("replay-scan").as(LavinMQ::AMQP::ReplayQueue)
          ids = [] of String
          q.each_envelope do |env|
            ids << env.message.properties.headers.not_nil!["x-replay-id"].to_s
          end
          ids.size.should eq 3
          target = ids[1]
          env = q.find_envelope_with_header(LavinMQ::Replay::HEADER_REPLAY_ID, target)
          env.should_not be_nil
          env.not_nil!.message.properties.headers.not_nil!["x-replay-id"].should eq target
        end
      end
    end

    it "find_envelope_with_header returns nil for an unknown id" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.queue("replay-miss", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          ch.basic_publish_confirm("m", "", "replay-miss", props: private_props)
          q = s.vhosts["/"].queue?("replay-miss").as(LavinMQ::AMQP::ReplayQueue)
          q.find_envelope_with_header(LavinMQ::Replay::HEADER_REPLAY_ID, "no-such-id").should be_nil
        end
      end
    end

    it "delete_envelope removes a ready message and updates count" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.queue("replay-del", durable: true,
            args: AMQP::Client::Arguments.new({"x-queue-type" => "replay"}))
          3.times do |i|
            ch.basic_publish_confirm("m-#{i}", "", "replay-del", props: private_props)
          end
          q = s.vhosts["/"].queue?("replay-del").as(LavinMQ::AMQP::ReplayQueue)
          q.message_count.should eq 3
          first_sp = nil
          q.each_envelope do |env|
            first_sp = env.segment_position
            break
          end
          q.delete_envelope(first_sp.not_nil!)
          q.message_count.should eq 2
          remaining = 0
          q.each_envelope { |_| remaining += 1 }
          remaining.should eq 2
        end
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
