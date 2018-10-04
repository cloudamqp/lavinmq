require "logger"
require "openssl"
require "socket"
require "../amqp"
require "../message"
require "./channel"
require "../user"

module AvalancheMQ
  abstract class Client
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

    def self.close_on_ok(socket, log)
      loop do
        AMQP::Frame.decode(socket) do |frame|
          log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          if frame.is_a?(AMQP::BodyFrame)
            log.debug "Skipping body"
            frame.body.skip(frame.body_size)
          end
          frame.is_a?(AMQP::Connection::Close | AMQP::Connection::CloseOk)
        end && break
      end
    rescue e : AMQP::FrameDecodeError
      log.warn { "#{e.inspect} when waiting for CloseOk" }
    ensure
      socket.close
    end

    def initialize(@name : String, @vhost : VHost, @log : Logger,
                   @client_properties = Hash(String, AMQP::Field).new)
      @connected_at = Time.now.epoch_ms
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
        if frame.is_a?(AMQP::BodyFrame)
          @log.debug "Skipping body"
          frame.body.skip(frame.body_size)
        end
      end
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
      send AMQP::Channel::OpenOk.new(frame.channel)
    end

    private def process_frame(frame)
      case frame
      when AMQP::Connection::Close
        send AMQP::Connection::CloseOk.new
        return false
      when AMQP::Connection::CloseOk
        @log.info "Disconnected"
        @log.debug { "Closing socket" }
        cleanup
        return false
      when AMQP::Channel::Open
        open_channel(frame)
      when AMQP::Channel::Close
        @channels.delete(frame.channel).try &.close
        send AMQP::Channel::CloseOk.new(frame.channel)
      when AMQP::Channel::CloseOk
        @channels.delete(frame.channel).try &.close
      when AMQP::Confirm::Select
        with_channel frame, &.confirm_select(frame)
      when AMQP::Exchange::Declare
        declare_exchange(frame)
      when AMQP::Exchange::Delete
        delete_exchange(frame)
      when AMQP::Exchange::Bind
        bind_exchange(frame)
      when AMQP::Exchange::Unbind
        unbind_exchange(frame)
      when AMQP::Queue::Declare
        declare_queue(frame)
      when AMQP::Queue::Bind
        bind_queue(frame)
      when AMQP::Queue::Unbind
        unbind_queue(frame)
      when AMQP::Queue::Delete
        delete_queue(frame)
      when AMQP::Queue::Purge
        purge_queue(frame)
      when AMQP::Basic::Publish
        start_publish(frame)
      when AMQP::HeaderFrame
        with_channel frame, &.next_msg_headers(frame)
      when AMQP::BodyFrame
        with_channel frame, &.add_content(frame)
      when AMQP::Basic::Consume
        consume(frame)
      when AMQP::Basic::Get
        basic_get(frame)
      when AMQP::Basic::Ack
        with_channel frame, &.basic_ack(frame)
      when AMQP::Basic::Reject
        with_channel frame, &.basic_reject(frame)
      when AMQP::Basic::Nack
        with_channel frame, &.basic_nack(frame)
      when AMQP::Basic::Cancel
        with_channel frame, &.cancel_consumer(frame)
      when AMQP::Basic::Qos
        with_channel frame, &.basic_qos(frame)
      when AMQP::Basic::Recover
        with_channel frame, &.basic_recover(frame)
      when AMQP::HeartbeatFrame
        # send AMQP::HeartbeatFrame.new
      else
        raise AMQP::NotImplemented.new(frame)
      end
      true
    rescue ex : AMQP::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      raise ex if ex.channel == 0
      close_channel(ex, 540_u16, "NOT_IMPLEMENTED")
      true
    rescue ex : KeyError
      raise ex unless frame.is_a? AMQP::MethodFrame
      @log.error { "Channel #{frame.channel} not open" }
      close_connection(frame, 504_u16, "CHANNEL_ERROR - Channel #{frame.channel} not open")
      true
    rescue ex : Exception
      raise ex unless frame.is_a? AMQP::MethodFrame
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
      @exclusive_queues.each &.close
      @channels.each_value &.close
      @channels.clear
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    def close(reason = "Broker shutdown")
      @log.debug "Gracefully closing"
      send AMQP::Connection::Close.new(320_u16, reason.to_s, 0_u16, 0_u16)
      @running = false
    end

    def closed?
      !@running
    end

    def close_channel(frame, code, text)
      send AMQP::Channel::Close.new(frame.channel, code, text, frame.class_id, frame.method_id)
      @channels[frame.channel].running = false
    end

    def close_connection(frame, code, text)
      send AMQP::Connection::Close.new(code, text, frame.class_id, frame.method_id)
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
      close_channel(frame, 403_u16, "ACCESS_REFUSED - #{text}")
    end

    def send_not_found(frame, text = "")
      close_channel(frame, 404_u16, "NOT_FOUND - #{text}")
    end

    def send_resource_locked(frame, text)
      close_channel(frame, 405_u16, "RESOURCE_LOCKED - #{text}")
    end

    def send_precondition_failed(frame, text)
      close_channel(frame, 406_u16, "PRECONDITION_FAILED - #{text}")
    end
  end
end
