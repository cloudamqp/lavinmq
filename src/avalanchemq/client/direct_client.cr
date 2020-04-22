require "./client"

module AvalancheMQ
  abstract class DirectClient < Client
    abstract def handle_frame(frame : Frame)
    Log = ::Log.for(self)

    def initialize(vhost : VHost, user : User, client_properties : AMQP::Table)
      name = "localhost:#{self.hash}"
      vhost.add_connection(self)
      super(name, vhost, user, client_properties)
      Log.info { "Connected" }
    end

    def details_tuple
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "direct",
        client_properties: @client_properties,
        vhost:             @vhost.name,
        protocol:          "Direct 0-9-1",
        name:              @name,
        state:             state,
      }
    end

    def channel_name_prefix
      @name
    end

    private def cleanup
      # noop
    end

    private def ensure_open_channel(frame)
      return if @channels[frame.channel]?.try(&.running?)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
    end

    def write(frame : AMQP::Frame)
      ensure_open_channel(frame)
      process_frame(frame)
    rescue ex : AMQP::Error::NotImplemented
      Log.error(exception: ex) { "#{ex} when reading handling frame" }
      if ex.channel > 0
        close_channel(ex, 540_u16, "Not implemented")
      else
        close_connection(ex, 540_u16, "Not implemented")
      end
    rescue ex : IO::Error | AMQP::Error::FrameDecode
      Log.info(exception: ex) { "Lost connection, while reading (#{ex.cause})" } unless closed?
      cleanup
    rescue ex : Exception
      Log.error(exception: ex) { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
      @running = false
    end

    def send(frame : AMQP::Frame)
      return false if closed?
      @send_oct_count += frame.bytesize + 8
      #Log.debug { "Send #{frame.inspect}" }
      handle_frame(frame)
      case frame
      when AMQP::Frame::Connection::CloseOk
        Log.info { "Disconnected" }
        cleanup
        false
      else true
      end
    rescue ex : IO::Error
      Log.info(exception: ex) { "Lost connection, while sending (#{ex})" }
      cleanup
      false
    rescue ex
      Log.error(exception: ex) { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        name: @name,
      }
    end

    def deliver(frame, msg)
      send frame
      send AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
      send AMQP::Frame::Body.new(frame.channel, msg.size.to_u32, msg.body_io)
    end
  end
end
