require "socket"
require "logger"
require "./message"
require "./client/*"

module AvalancheMQ
  class Client
    getter :socket, :vhost, :channels, log

    @remote_address : Socket::IPAddress
    @log : Logger

    def initialize(@socket : TCPSocket, @vhost : VHost, server_log : Logger)
      @remote_address = @socket.remote_address
      @log = server_log.dup
      @log.progname = "Client[#{@remote_address}]"
      @channels = Hash(UInt16, Client::Channel).new
      @outbox = ::Channel(AMQP::Frame).new(1000)
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
      spawn send_loop, name: "Client#send_loop #{@remote_address}"
    end

    def self.start(socket, vhosts, log)
      start = Bytes.new(8)
      bytes = socket.read_fully(start)

      if start != AMQP::PROTOCOL_START
        socket.write AMQP::PROTOCOL_START
        socket.close
        return
      end

      socket.write AMQP::Connection::Start.new.to_slice
      start_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::StartOk)
      socket.write AMQP::Connection::Tune.new(heartbeat: 0_u16).to_slice
      tune_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::TuneOk)
      open = AMQP::Frame.decode(socket).as(AMQP::Connection::Open)
      if vhost = vhosts[open.vhost]?
        socket.write AMQP::Connection::OpenOk.new.to_slice
        log.info "Accepting connection from #{socket.remote_address} to vhost #{open.vhost}"
        return self.new(socket, vhost, log)
      else
        log.warn "Access denied for #{socket.remote_address} to vhost #{open.vhost}"
        socket.write AMQP::Connection::Close.new(530_u16, "ACCESS_REFUSED",
                                                 open.class_id, open.method_id).to_slice
        socket.close
        return nil
      end
    rescue ex : IO::EOFError | Errno
      log.warn "#{ex.to_s} while establishing connection"
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
        #@log.debug { "<= #{frame.inspect}" }
        @socket.write frame.to_slice
        #@socket.flush unless frame.is_a? AMQP::Basic::Deliver | AMQP::HeaderFrame
        case frame
        when AMQP::Connection::Close
          @log.debug { "Closing write socket" }
          @socket.close_write
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
    rescue ex : IO::Error | Errno | IO::Timeout
      @log.debug { "#{ex} when writing to socket" }
      @log.debug { "Closing socket" }
      @socket.close
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
        send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                      "Queue doesn't exists",
                                      frame.class_id, frame.method_id)
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                      "Exchange doesn't exists",
                                      frame.class_id, frame.method_id)
      else
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Queue::BindOk.new(frame.channel)
        end
      end
    end

    private def unbind_queue(frame)
      if @vhost.queues.has_key? frame.queue_name && @vhost.exchanges.has_key? frame.exchange_name
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Queue::UnbindOk.new(frame.channel)
        end
      else
        send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                      "Queue or exchange does not exists",
                                      frame.class_id, frame.method_id)
      end
    end

    private def purge_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        messages_purged = q.purge
        unless frame.no_wait
          send AMQP::Queue::PurgeOk.new(frame.channel, messages_purged)
        end
      else
        send_not_found(frame)
      end
    end

    private def read_loop
      i = 0
      loop do
        frame = AMQP::Frame.decode @socket
        #@log.debug { "=> #{frame.inspect}" }
        case frame
        when AMQP::Connection::Close
          send AMQP::Connection::CloseOk.new
          break
        when AMQP::Connection::CloseOk
          cleanup
          break
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
          @channels[frame.channel].next_msg_headers(frame.body_size, frame.properties)
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
        else @log.error "Unhandled frame #{frame.inspect}"
        end
        if (i += 1) % 1000 == 0
          @log.debug "read_loop yielding"
          Fiber.yield
        end
      end
      @log.debug { "Close read socket" }
      @socket.close_read
    rescue ex : KeyError
      @log.debug { "Channel already closed" }
    rescue ex : IO::Error | Errno
      @log.debug { "#{ex} when reading from socket" }
      unless @outbox.closed?
        @log.debug { "Closing outbox" }
        @outbox.close # Notifies send_loop to close up shop
      end
    end

    private def send_not_found(frame)
      send AMQP::Channel::Close.new(frame.channel, 404_u16, "Not found",
                                    frame.class_id, frame.method_id)
    end

    def send(frame : AMQP::Frame)
      @outbox.send frame
    end
  end
end
