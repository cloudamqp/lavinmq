require "./spec_helper"
require "amqp-client"

describe LavinMQ::AMQP::Channel do
  describe "body buffer" do
    it "is kept while the channel publishes, released after a pass without publishes, and reallocated on the next publish" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("body-buffer")
          ch.confirm_select
          ch.basic_publish_confirm("x" * 10_000, "", q.name)
          server_ch = s.connections.first.as(LavinMQ::AMQP::Client).channels.first.as(LavinMQ::AMQP::Channel)
          s.release_idle_buffers # published since the last pass: kept
          server_ch.@body_buffer.get.should_not be_nil
          s.release_idle_buffers # nothing published since: released
          server_ch.@body_buffer.get.should be_nil
          ch.basic_publish_confirm("y" * 10_000, "", q.name)
          server_ch.@body_buffer.get.should_not be_nil
          first = q.get(no_ack: true).should_not be_nil
          first.body_io.to_s.should eq "x" * 10_000
          second = q.get(no_ack: true).should_not be_nil
          second.body_io.to_s.should eq "y" * 10_000
        end
      end
    end
  end

  describe LavinMQ::AMQP::Channel::PublishBody do
    it "reads, seeks and rewinds like an IO::Memory over the slice it was reset to" do
      body = LavinMQ::AMQP::Channel::PublishBody.new
      body.reset("hello world".to_slice)
      body.gets_to_end.should eq "hello world"
      body.seek(-5, IO::Seek::Current)
      body.gets_to_end.should eq "world"
      body.rewind
      body.to_slice.should eq "hello world".to_slice
      body.reset(Bytes.empty)
      body.size.should eq 0
      body.gets_to_end.should eq ""
    end
  end

  it "should respect consumer_max_per_channel config" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 1
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      expect_raises(AMQP::Client::Channel::ClosedException, /RESOURCE_ERROR/) do
        channel.basic_consume("test:queue:2") { }
      end
    end
  end

  it "consumer_max_per_channel = 0 allows unlimited consumers" do
    config = LavinMQ::Config.new
    config.max_consumers_per_channel = 0
    with_amqp_server(config: config) do |s|
      connection = AMQP::Client.new(port: amqp_port(s)).connect
      channel = connection.channel
      channel.queue("test:queue:1")
      channel.queue("test:queue:2")
      channel.basic_consume("test:queue:1") { }
      channel.basic_consume("test:queue:1") { }
    end
  end

  it "should requeue a basic_get message when the channel closes during the get" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("basic-get-close")
        q.publish_confirm("m")
        sq = s.vhosts["/"].queue("basic-get-close")
        server_ch = s.connections.first.as(LavinMQ::AMQP::Client).channels.first.as(LavinMQ::AMQP::Channel)
        got = Channel(Nil).new
        # Hold the queue's store lock so the get waits for it, then close the
        # channel while it waits, as the management API can
        sq.@msg_store_lock.synchronize do
          spawn do
            q.get(no_ack: false)
          rescue AMQP::Client::Channel::ClosedException
          ensure
            got.send nil
          end
          wait_for { sq.@get_count.get(:relaxed) == 1 }
          server_ch.close("closed during basic.get")
        end
        got.receive
        sq.unacked_count.should eq 0
        sq.message_count.should eq 1
        ch.@connection.closed?.should be_false
      end
    end
  end
end
