require "./spec_helper"
require "amqp-client"

describe LavinMQ::AMQP::Channel do
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

  it "tolerates in-flight frames between server-sent Channel::Close and client CloseOk" do
    with_amqp_server do |s|
      io = TCPSocket.new("localhost", amqp_port(s))
      io.read_timeout = 5.seconds

      io.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
      io.flush
      stream = AMQ::Protocol::Stream.new(io)
      stream.next_frame.as(AMQ::Protocol::Frame::Connection::Start)
      response = "\u0000guest\u0000guest"
      io.write_bytes(AMQ::Protocol::Frame::Connection::StartOk.new(
        AMQ::Protocol::Table.new, "PLAIN", response, ""),
        IO::ByteFormat::NetworkEndian)
      io.flush
      tune = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Tune)
      io.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(
        channel_max: tune.channel_max, frame_max: tune.frame_max, heartbeat: 0_u16),
        IO::ByteFormat::NetworkEndian
      io.write_bytes AMQ::Protocol::Frame::Connection::Open.new("/"), IO::ByteFormat::NetworkEndian
      io.flush
      stream.next_frame.as(AMQ::Protocol::Frame::Connection::OpenOk)

      io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(1_u16), IO::ByteFormat::NetworkEndian
      io.flush
      stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)

      # Trigger a server-initiated channel close via passive declare on a missing queue
      io.write_bytes AMQ::Protocol::Frame::Queue::Declare.new(
        1_u16, 0_u16, "nonexistent-#{rand}", passive: true,
        durable: false, exclusive: false, auto_delete: false, no_wait: false,
        arguments: AMQ::Protocol::Table.new), IO::ByteFormat::NetworkEndian
      io.flush
      close = stream.next_frame.as(AMQ::Protocol::Frame::Channel::Close)
      close.reply_code.should eq 404

      # Simulate the stress-test race: client's in-flight Basic::Ack arrives
      # after the server's Channel::Close but before the client sends CloseOk.
      io.write_bytes AMQ::Protocol::Frame::Basic::Ack.new(1_u16, 1_u64, false),
        IO::ByteFormat::NetworkEndian
      io.flush

      io.write_bytes AMQ::Protocol::Frame::Channel::CloseOk.new(1_u16), IO::ByteFormat::NetworkEndian
      io.flush

      # Prove the connection is still alive by opening a new channel
      io.write_bytes AMQ::Protocol::Frame::Channel::Open.new(2_u16), IO::ByteFormat::NetworkEndian
      io.flush
      stream.next_frame.as(AMQ::Protocol::Frame::Channel::OpenOk)
    ensure
      io.try &.close
    end
  end
end
