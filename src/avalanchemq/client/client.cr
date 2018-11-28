require "logger"
require "openssl"
require "socket"
require "../message"
require "./channel"
require "../user"
require "../stats"

module AvalancheMQ
  abstract class Client
    include Stats

    abstract def send(frame : AMQP::Frame)
    abstract def to_json(json : JSON::Builder)
    abstract def connection_details
    abstract def deliver(frame : AMQP::Frame, msg : Message)
    abstract def channel_name_prefix
    private abstract def cleanup

    setter direct_reply_consumer_tag
    getter vhost, channels, log, exclusive_queues,
      name, direct_reply_consumer_tag

    @client_properties : Hash(String, AMQP::Field)
    @connected_at : Int64
    @direct_reply_consumer_tag : String?
    @log : Logger
    @running = true
    rate_stats(%w(send_oct recv_oct))

    def self.close_on_ok(socket, log)
      loop do
        AMQP::Frame.from_io(socket, IO::ByteFormat::NetworkEndian) do |frame|
          log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          if frame.is_a?(AMQP::Frame::Body)
            log.debug "Skipping body"
            frame.body.skip(frame.body_size)
          end
          frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk)
        end && break
      end
    rescue IO::EOFError
      log.debug { "Client closed socket without sending CloseOk" }
    rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
      log.warn { "#{ex.inspect} when waiting for CloseOk" }
    ensure
      socket.close
    end

    def initialize(@name : String, @vhost : VHost, @log : Logger,
                   @client_properties = Hash(String, AMQP::Field).new)
      @connected_at = Time.utc_now.to_unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @exclusive_queues = Array(Queue).new
      @log.info "Connected"
    end

    def user
      nil
    end

    private def with_channel(frame)
      ch = @channels[frame.channel]
      if ch.running?
        yield ch
      else
        @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
        if frame.is_a?(AMQP::Frame::Body)
          @log.debug "Skipping body"
          frame.body.skip(frame.body_size)
        end
      end
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
      send AMQP::Frame::Channel::OpenOk.new(frame.channel)
    end

    private def process_frame(frame)
      @recv_oct_count += frame.bytesize + 8
      case frame
      when AMQP::Frame::Connection::Close
        send AMQP::Frame::Connection::CloseOk.new
        return false
      when AMQP::Frame::Connection::CloseOk
        @log.info "Disconnected"
        @log.debug { "Closing socket" }
        cleanup
        return false
      when AMQP::Frame::Channel::Open
        open_channel(frame)
      when AMQP::Frame::Channel::Close
        @channels.delete(frame.channel).try &.close
        send AMQP::Frame::Channel::CloseOk.new(frame.channel)
      when AMQP::Frame::Channel::CloseOk
        @channels.delete(frame.channel).try &.close
      when AMQP::Frame::Confirm::Select
        with_channel frame, &.confirm_select(frame)
      when AMQP::Frame::Exchange::Declare
        declare_exchange(frame)
      when AMQP::Frame::Exchange::Delete
        delete_exchange(frame)
      when AMQP::Frame::Exchange::Bind
        bind_exchange(frame)
      when AMQP::Frame::Exchange::Unbind
        unbind_exchange(frame)
      when AMQP::Frame::Queue::Declare
        declare_queue(frame)
      when AMQP::Frame::Queue::Bind
        bind_queue(frame)
      when AMQP::Frame::Queue::Unbind
        unbind_queue(frame)
      when AMQP::Frame::Queue::Delete
        delete_queue(frame)
      when AMQP::Frame::Queue::Purge
        purge_queue(frame)
      when AMQP::Frame::Basic::Publish
        start_publish(frame)
      when AMQP::Frame::Header
        with_channel frame, &.next_msg_headers(frame)
      when AMQP::Frame::Body
        with_channel frame, &.add_content(frame)
      when AMQP::Frame::Basic::Consume
        consume(frame)
      when AMQP::Frame::Basic::Get
        basic_get(frame)
      when AMQP::Frame::Basic::Ack
        with_channel frame, &.basic_ack(frame)
      when AMQP::Frame::Basic::Reject
        with_channel frame, &.basic_reject(frame)
      when AMQP::Frame::Basic::Nack
        with_channel frame, &.basic_nack(frame)
      when AMQP::Frame::Basic::Cancel
        with_channel frame, &.cancel_consumer(frame)
      when AMQP::Frame::Basic::Qos
        with_channel frame, &.basic_qos(frame)
      when AMQP::Frame::Basic::Recover
        with_channel frame, &.basic_recover(frame)
      when AMQP::Frame::Heartbeat
        # send AMQP::HeartbeatFrame.new
      else
        raise AMQP::Error::NotImplemented.new(frame)
      end
      true
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      raise ex if ex.channel == 0
      close_channel(ex, 540_u16, "NOT_IMPLEMENTED")
      true
    rescue ex : KeyError
      raise ex unless frame.is_a? AMQP::Frame::Method
      @log.error { "Channel #{frame.channel} not open" }
      close_connection(frame, 504_u16, "CHANNEL_ERROR - Channel #{frame.channel} not open")
      true
    rescue ex : Exception
      raise ex unless frame.is_a? AMQP::Frame::Method
      @log.error { "#{ex.inspect}, when processing frame" }
      @log.debug { ex.inspect_with_backtrace }
      close_channel(frame, 541_u16, "INTERNAL_ERROR")
      true
    end

    def cleanup
      @running = false
      @log.debug "Yielding before cleaning up"
      Fiber.yield
      @log.debug "Cleaning up"
      @exclusive_queues.each(&.close)
      @exclusive_queues.clear
      @channels.each_value &.close
      @channels.clear
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    def close(reason = nil)
      reason ||= "Connection closed"
      @log.info { "Closing, #{reason}" }
      send AMQP::Frame::Connection::Close.new(320_u16, reason.to_s, 0_u16, 0_u16)
      @running = false
    end

    def closed?
      !@running
    end

    def close_channel(frame, code, text)
      case frame
      when AMQP::Frame::Header
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, 0, 0)
      else
        send AMQP::Frame::Channel::Close.new(frame.channel, code, text, frame.class_id, frame.method_id)
      end
      @channels[frame.channel].running = false
    end

    def close_connection(frame, code, text)
      @log.info { "Closing, #{text}" }
      send AMQP::Frame::Connection::Close.new(code, text, frame.class_id, frame.method_id)
      @running = false
    end

    def direct_reply_channel
      if direct_reply_consumer_tag
        @vhost.direct_reply_channels[direct_reply_consumer_tag]?
      end
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def send_access_refused(frame, text)
      @log.warn { "Access refused channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 403_u16, "ACCESS_REFUSED - #{text}")
    end

    def send_not_found(frame, text = "")
      @log.warn { "Not found channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 404_u16, "NOT_FOUND - #{text}")
    end

    def send_resource_locked(frame, text)
      @log.warn { "Resource locked channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 405_u16, "RESOURCE_LOCKED - #{text}")
    end

    def send_precondition_failed(frame, text)
      @log.warn { "Precondition failed channel=#{frame.channel} reason=\"#{text}\"" }
      close_channel(frame, 406_u16, "PRECONDITION_FAILED - #{text}")
    end
  end
end
