require "socket"
require "logger"
require "./message"
require "./client/*"

module AvalancheMQ
  class Client
    getter socket, vhost, channels, log, max_frame_size

    @log : Logger

    def initialize(@socket : TCPSocket | OpenSSL::SSL::Socket,
                   @remote_address : Socket::IPAddress,
                   @vhost : VHost,
                   @max_frame_size : UInt32)
      @log = @vhost.log.dup
      @log.progname = "Client[#{@remote_address}]"
      @channels = Hash(UInt16, Client::Channel).new
      @outbox = ::Channel(AMQP::Frame).new(1000)
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
      spawn send_loop, name: "Client#send_loop #{@remote_address}"
    end

    def self.start(socket, remote_address, vhosts, log)
      start = Bytes.new(8)
      bytes = socket.read_fully(start)

      if start != AMQP::PROTOCOL_START
        socket.write AMQP::PROTOCOL_START
        socket.close
        return
      end

      socket.write AMQP::Connection::Start.new.to_slice
      socket.flush
      start_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::StartOk)
      socket.write AMQP::Connection::Tune.new(heartbeat: 0_u16).to_slice
      socket.flush
      tune_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::TuneOk)
      open = AMQP::Frame.decode(socket).as(AMQP::Connection::Open)
      if vhost = vhosts[open.vhost]?
        socket.write AMQP::Connection::OpenOk.new.to_slice
        socket.flush
        log.info "Accepting connection from #{remote_address} to vhost #{open.vhost}"
        return self.new(socket, remote_address, vhost, tune_ok.frame_max)
      else
        log.warn "Access denied for #{remote_address} to vhost #{open.vhost}"
        socket.write AMQP::Connection::Close.new(530_u16, "ACCESS_REFUSED",
                                                 open.class_id, open.method_id).to_slice
        socket.close
        return nil
      end
    rescue ex : AMQP::FrameDecodeError
      log.warn "#{ex.cause.inspect} while establishing connection"
      nil
    end

    def close
      @log.debug "Gracefully closing"
      send AMQP::Connection::Close.new(320_u16, "Broker shutdown", 0_u16, 0_u16)
    end

    def cleanup
      @log.debug "Cleaning up"
      @channels.each_value &.close
      @channels.clear
      @on_close_callback.try &.call(self)
      @on_close_callback = nil
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def to_json(json : JSON::Builder)
      {
        address: @remote_address.to_s,
        channels: @channels.size,
      }.to_json(json)
    end

    private def send_loop
      i = 0
      loop do
        frame = @outbox.receive
        @log.debug { "Send #{frame.inspect}"}
        @socket.write frame.to_slice
        unless frame.is_a?(AMQP::Basic::Deliver) || frame.is_a?(AMQP::HeaderFrame)
          @socket.flush
        end
        case frame
        when AMQP::Connection::Close
          break
        when AMQP::Connection::CloseOk
          @log.debug { "Closing socket" }
          @socket.close
          cleanup
          break
        end
        if (i += 1) % 1000 == 0
          @log.debug "send_loop yielding"
          Fiber.yield
        end
      end
    rescue ex : ::Channel::ClosedError
      @log.debug { "#{ex}, when waiting for frames to send" }
      @log.debug { "Closing socket" }
      @socket.close
      cleanup
    rescue ex : IO::Error | Errno
      @log.debug { "#{ex} when writing to socket" }
      cleanup
    ensure
      @log.debug { "Closing outbox" }
      @outbox.close
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
      send AMQP::Channel::OpenOk.new(frame.channel)
    end

    private def declare_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.passive ||
            e.type == frame.exchange_type &&
            e.durable == frame.durable &&
            e.auto_delete == frame.auto_delete &&
            e.internal == frame.internal &&
            e.arguments == frame.arguments
          unless frame.no_wait
            send AMQP::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                        "Existing exchange declared with other arguments",
                                        frame.class_id, frame.method_id)
        end
      elsif frame.passive
        send_not_found(frame)
      else
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        end
      end
    end

    private def delete_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Exchange::DeleteOk.new(frame.channel)
        end
      else
        send_not_found(frame)
      end
    end

    private def delete_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if frame.if_unused && !q.consumer_count.zero?
          send AMQP::Channel::Close.new(frame.channel, 403_u16, "In use",
                                        frame.class_id, frame.method_id)
          return
        end
        if frame.if_empty && !q.message_count.zero?
          send AMQP::Channel::Close.new(frame.channel, 403_u16, "Not empty",
                                        frame.class_id, frame.method_id)
          return
        end
        size = q.message_count
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Queue::DeleteOk.new(frame.channel, size)
        end
      else
        send_not_found(frame)
      end
    end

    private def declare_queue(frame)
      if frame.queue_name.empty?
        frame.queue_name = "amq.gen-#{Random::Secure.urlsafe_base64(24)}"
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if frame.passive ||
            q.durable == frame.durable &&
            q.exclusive == frame.exclusive &&
            q.auto_delete == frame.auto_delete &&
            q.arguments == frame.arguments
          unless frame.no_wait
            send AMQP::Queue::DeclareOk.new(frame.channel, q.name,
                                            q.message_count, q.consumer_count)
          end
        else
          send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                        "Existing queue declared with other arguments",
                                        frame.class_id, frame.method_id)
        end
      elsif frame.passive
        send_not_found(frame)
      else
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
        end
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      else
        @vhost.apply(frame)
        send AMQP::Queue::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      else
        @vhost.apply(frame)
        send AMQP::Queue::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def bind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      else
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Exchange::BindOk.new(frame.channel)
        end
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      else
        @vhost.apply(frame)
        send AMQP::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        messages_purged = q.purge
        send AMQP::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
      else
        send_not_found(frame, "Queue #{frame.queue_name} not found")
      end
    end

    private def read_loop
      i = 0
      loop do
        frame = AMQP::Frame.decode @socket
        @log.debug { "Read #{frame.inspect}" }
        ok = process_frame(frame)
        break unless ok
        if (i += 1) % 1000 == 0
          @log.debug "read_loop yielding"
          Fiber.yield
        end
      end
    rescue ex : AMQP::NotImplemented
      @log.error { "#{ex} when reading from socket" }
      if ex.channel > 0
        send AMQP::Channel::Close.new(ex.channel, 540_u16, "Not implemented", ex.class_id, ex.method_id)
      else
        send AMQP::Connection::Close.new(540_u16, "Not implemented", ex.class_id, ex.method_id)
      end
    rescue ex : AMQP::FrameDecodeError
      @log.error { "#{ex.cause} when reading from socket" }
      @log.debug { "Closing outbox" }
      @outbox.close # Notifies send_loop to close up shop
    rescue ex : Exception
      @log.error { "#{ex.inspect}, in read loop" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    private def process_frame(frame)
      case frame
      when AMQP::Connection::Close
        send AMQP::Connection::CloseOk.new
        return false
      when AMQP::Connection::CloseOk
        @log.debug { "Closing socket" }
        @socket.close
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
        @channels[frame.channel].confirm_select(frame)
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
        @channels[frame.channel].start_publish(frame)
      when AMQP::HeaderFrame
        @channels[frame.channel].next_msg_headers(frame)
      when AMQP::BodyFrame
        @channels[frame.channel].add_content(frame)
      when AMQP::Basic::Consume
        @channels[frame.channel].consume(frame)
      when AMQP::Basic::Get
        @channels[frame.channel].basic_get(frame)
      when AMQP::Basic::Ack
        @channels[frame.channel].basic_ack(frame)
      when AMQP::Basic::Reject
        @channels[frame.channel].basic_reject(frame)
      when AMQP::Basic::Nack
        @channels[frame.channel].basic_nack(frame)
      when AMQP::Basic::Cancel
        @channels[frame.channel].cancel_consumer(frame)
      when AMQP::Basic::Qos
        @channels[frame.channel].basic_qos(frame)
      when AMQP::HeartbeatFrame
        send AMQP::HeartbeatFrame.new
      else
        raise AMQP::NotImplemented.new(frame)
      end
      true
    rescue ex : AMQP::NotImplemented
      @log.error { "#{frame.inspect}, not implemented" }
      raise ex if ex.channel == 0
      send AMQP::Channel::Close.new(ex.channel, 540_u16, "Not implemented", ex.class_id, ex.method_id)
      true
    rescue ex : KeyError
      raise ex unless frame.is_a? AMQP::MethodFrame
      @log.error { "Channel #{frame.channel} not open" }
      send AMQP::Connection::Close.new(504_u16, "Channel #{frame.channel} not open",
                                       frame.class_id, frame.method_id)
      true
    rescue ex : Exception
      raise ex unless frame.is_a? AMQP::MethodFrame
      @log.error { "#{ex.inspect}, when processing frame" }
      send AMQP::Channel::Close.new(frame.channel, 541_u16, "Internal error",
                                    frame.class_id, frame.method_id)
      true
    end

    def send_not_found(frame, reply_text = "Not found")
      send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send_precondition_failed(frame, reply_text = "Precondition failed")
      send AMQP::Channel::Close.new(frame.channel, 406_u16, reply_text,
                                    frame.class_id, frame.method_id)
    end

    def send(frame : AMQP::Frame)
      @outbox.send frame
      if frame.is_a? AMQP::Channel::Close || frame.is_a? AMQP::Connection::Close
        Fiber.yield
      end
    end
  end
end
