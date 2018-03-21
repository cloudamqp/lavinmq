require "socket"
require "./message"
require "./client/*"

module AMQPServer
  class Client
    getter :socket, :vhost, :channels

    @remote_address : Socket::IPAddress

    def initialize(@socket : TCPSocket, @vhost : VHost, @log : Logger)
      @remote_address = @socket.remote_address
      @channels = Hash(UInt16, Client::Channel).new
      @outbox = ::Channel(AMQP::Frame | Nil).new(16)
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
      socket.write AMQP::Connection::Tune.new(heartbeat: 60_u16).to_slice
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
    rescue ex
      log.error ex.inspect_with_backtrace
      nil
    end

    def close(server_initiated = true)
      return if @outbox.closed?
      @log.info "Closing client #{@remote_address}"
      @channels.each_value &.close
      if cb = @on_close_callback
        cb.call self
      end
      if server_initiated
        send AMQP::Connection::Close.new(320_u16, "Broker shutdown", 0_u16, 0_u16)
      else
        send AMQP::Connection::CloseOk.new
      end
      @outbox.close
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
      loop do
        frame = @outbox.receive
        @log.debug { "<= #{frame.inspect}" }
        break if frame.nil?
        @socket.write frame.to_slice
      end
      @log.info "closeing write @ #{@remote_address}"
      @socket.close_write
      @log.info "closed write @ #{@remote_address}"
    rescue ex : IO::Error | Errno | ::Channel::ClosedError
      @log.info "Client connection #{@remote_address} write closed: #{ex.inspect}"
    ensure
      close
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self)
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
        send AMQP::Channel::Close.new(frame.channel, 404_u16, "NOT FOUND",
                                      frame.class_id, frame.method_id)
      else
        @vhost.apply(frame)
        unless frame.no_wait
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        end
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
        send AMQP::Channel::Close.new(frame.channel, 404_u16, "Not found",
                                      frame.class_id, frame.method_id)
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
        send AMQP::Channel::Close.new(frame.channel, 404_u16, "NOT FOUND",
                                      frame.class_id, frame.method_id)
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

    private def read_loop
      loop do
        frame = AMQP::Frame.decode @socket
        @log.debug { "=> #{frame.inspect}" }
        case frame
        when AMQP::Connection::Close
          close(false)
          break
        when AMQP::Connection::CloseOk.new
          close(true)
          break
        when AMQP::Channel::Open
          open_channel(frame)
        when AMQP::Channel::Close
          if ch = @channels.delete(frame.channel)
            ch.close
          end
          send AMQP::Channel::CloseOk.new(frame.channel)
        when AMQP::Channel::CloseOk
          if ch = @channels.delete(frame.channel)
            ch.close
          end
        when AMQP::Exchange::Declare
          declare_exchange(frame)
        when AMQP::Queue::Declare
          declare_queue(frame)
        when AMQP::Queue::Bind
          bind_queue(frame)
        when AMQP::Queue::Unbind
          unbind_queue(frame)
        when AMQP::Queue::Delete
          delete_queue(frame)
        when AMQP::Basic::Publish
          @channels[frame.channel].start_publish(frame.exchange, frame.routing_key)
        when AMQP::HeaderFrame
          @channels[frame.channel].next_msg_headers(frame.body_size, frame.properties)
        when AMQP::BodyFrame
          @channels[frame.channel].add_content(frame.body)
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
        when AMQP::Basic::Qos
          @channels[frame.channel].basic_qos(frame)
        when AMQPServer::AMQP::HeartbeatFrame
          send AMQPServer::AMQP::HeartbeatFrame.new
        else @log.error "[ERROR] Unhandled frame #{frame.inspect}"
        end
      end
      @socket.close_read
    rescue ex : IO::Error | Errno | ::Channel::ClosedError
      @log.error "Client connection #{@remote_address} read_loop closed: #{ex.inspect}"
    ensure
      close
    end

    def send(frame : AMQP::Frame | Nil)
      @outbox.send frame
    end
  end
end
