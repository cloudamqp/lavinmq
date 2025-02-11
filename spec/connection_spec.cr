require "./spec_helper"

# AMQP::Client is too good at handling errors, so we need to make it unsafe
class AMQP::Client::UnsafeConnection < AMQP::Client::Connection
  # It does not allow the client to send a channel_max or frame_max higher than the server's
  # We need to test that the server can handle this case
  private def self.tune(io, channel_max, frame_max, heartbeat)
    _tune = Frame.from_io(io) do |f|
      case f
      when Frame::Connection::Tune  then f
      when Frame::Connection::Close then raise ClosedException.new(f)
      else                               raise Error::UnexpectedFrame.new(f)
      end
    end
    # channel_max = tune.channel_max.zero? ? channel_max : Math.min(tune.channel_max, channel_max)
    # frame_max = tune.frame_max.zero? ? frame_max : Math.min(tune.frame_max, frame_max)
    io.write_bytes Frame::Connection::TuneOk.new(channel_max: channel_max,
      frame_max: frame_max,
      heartbeat: heartbeat),
      IO::ByteFormat::NetworkEndian
    {channel_max, frame_max, heartbeat}
  end

  # AMQP::Client protects against the client opening more than channel_max channels
  # We need to test that the server can handle this case
  def channel(id : Int? = nil)
    ch = nil
    @channels_lock.synchronize do
      if id
        # raise "channel_max reached" if id > @channel_max
        if ch = @channels.fetch(id, nil)
          return ch
        else
          ch = @channels[id] = Channel.new(self, id)
        end
      end
      (1_u16..).each do |i| # (1.up_to(@channel_max)) in safe implementation
        next if @channels.has_key? i
        ch = @channels[i] = Channel.new(self, i)
        break
      end
    end
    # raise "channel_max reached" if ch.nil?
    # We must open the channel outside of the lock to avoid deadlocks
    raise "bug in unsafe spec #channel implementation" if ch.nil?
    ch.open
  end
end

class AMQP::Client::UnsafeClient < AMQP::Client
  def connect_unsafe : Connection
    socket = connect_tcp
    AMQP::Client::UnsafeConnection.start(socket, @user, @password, @vhost, @channel_max,
      @frame_max, @heartbeat, @connection_information, @name)
  end
end

describe LavinMQ::Server do
  describe "channel_max" do
    it "should not accpet a channel_max from the client lower than the server config" do
      with_amqp_server do |s|
        server_channel_max = LavinMQ::Config.instance.channel_max
        client_channel_max = server_channel_max + 1
        expect_raises(AMQP::Client::Connection::ClosedException, /failed to negotiate connection parameters/) do
          AMQP::Client::UnsafeClient.new(port: amqp_port(s), channel_max: client_channel_max).connect_unsafe
        end
      end
    end

    it "should not allow client to set channel_max to zero (unlimited) if the server has a limit" do
      with_amqp_server do |s|
        expect_raises(AMQP::Client::Connection::ClosedException, /failed to negotiate connection parameters/) do
          AMQP::Client::UnsafeClient.new(port: amqp_port(s), channel_max: 0).connect_unsafe
        end
      end
    end

    it "should allow client to set channel_max to zero (unlimited) if the server has no limit" do
      config = LavinMQ::Config.new
      config.channel_max = 0
      with_amqp_server(config: config) do |s|
        AMQP::Client::UnsafeClient.new(port: amqp_port(s), channel_max: 0).connect_unsafe
        s.connections.first.channel_max.should eq 0
      end
    end

    # TODO: needs new amqp-client release, current version deadlocks
    pending "should not allow client to create more channels than the negotiated max" do
      with_amqp_server do |s|
        conn = AMQP::Client::UnsafeClient.new(port: amqp_port(s), channel_max: 1).connect_unsafe
        conn.channel
        expect_raises(AMQP::Client::Connection::ClosedException, /number of channels opened \(1\) has reached the negotiated channel_max \(1\)/) do
          conn.channel
        end
      end
    end
  end

  describe "frame_max" do
    it "should not accpet a frame_max from the client lower than the server config" do
      with_amqp_server do |s|
        server_frame_max = LavinMQ::Config.instance.frame_max
        client_frame_max = server_frame_max + 1
        expect_raises(AMQP::Client::Connection::ClosedException, /failed to negotiate connection parameters/) do
          AMQP::Client::UnsafeClient.new(port: amqp_port(s), frame_max: client_frame_max).connect_unsafe
        end
      end
    end

    it "should not allow client to set frame_max to zero (unlimited) if the server has a limit" do
      with_amqp_server do |s|
        expect_raises(AMQP::Client::Connection::ClosedException, /failed to negotiate connection parameters/) do
          AMQP::Client::UnsafeClient.new(port: amqp_port(s), frame_max: 0).connect_unsafe
        end
      end
    end

    it "should allow client to set frame_max to zero (unlimited) if the server has no limit" do
      config = LavinMQ::Config.new
      config.frame_max = 0
      with_amqp_server(config: config) do |s|
        AMQP::Client::UnsafeClient.new(port: amqp_port(s), frame_max: 0).connect_unsafe
        s.connections.first.max_frame_size.should eq 0
      end
    end

    # TODO: needs new amqp-client release, current version deadlocks
    pending "should not allow client to send a frame larger than the negotiated frame_max" do
      with_amqp_server do |s|
        conn = AMQP::Client.new(port: amqp_port(s)).connect
        ch = conn.channel
        frame_max = LavinMQ::Config.instance.frame_max
        bytes = frame_max + 1
        expect_raises(AMQP::Client::Connection::ClosedException, /frame size #{bytes} exceeded max #{frame_max}/) do
          conn.unsafe_write AMQ::Protocol::Frame::Basic::Publish.new(ch.id, 0_u16, "amq.direct", "test", false, false)
          conn.unsafe_write AMQ::Protocol::Frame::Header.new(ch.id, 60_u16, 0_u16, bytes.to_u64, AMQ::Protocol::Properties.new)
          conn.unsafe_write AMQ::Protocol::Frame::BytesBody.new(ch.id, bytes, Slice.new(bytes.to_i32, 0_u8))
          conn.channel # We need to do something blocking on the channel to trigger the error
        end
      end
    end
  end
end
