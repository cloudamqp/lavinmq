require "socket"
require "./message"
require "./client/*"

module AMQPServer
  class Client
    getter :socket, :vhost, :channels

    def initialize(@socket : TCPSocket, @vhost : VHost)
      @delivery_tag = 0_u64
      @channels = Hash(UInt16, Client::Channel).new
      @send_chan = ::Channel(AMQP::Frame).new(16)
      spawn read_loop
      spawn send_loop
    end

    def self.start(socket, vhosts)
      start = Bytes.new(8)
      bytes = socket.read_fully(start)

      if start != AMQP::PROTOCOL_START
        socket.write AMQP::PROTOCOL_START
        socket.close
        return
      end

      socket.write AMQP::Connection::Start.new.to_slice
      start_ok = AMQP::Frame.decode socket
      socket.write AMQP::Connection::Tune.new(heartbeat: 60_u16).to_slice
      tune_ok = AMQP::Frame.decode socket
      open = AMQP::Frame.decode(socket).as(AMQP::Connection::Open)
      if vhost = vhosts[open.vhost]?
          socket.write AMQP::Connection::OpenOk.new.to_slice
        return self.new(socket, vhost)
      else
        print "Access denied for", socket.remote_address, " to vhost ", open.vhost, "\n"
        socket.write AMQP::Connection::Close.new(530_u16, "ACCESS_REFUSED",
                                                 open.class_id, open.method_id).to_slice
        socket.close
        return nil
      end
    rescue ex
      puts ex.inspect
      ex.backtrace.each do |l|
        puts l
      end
      nil
    end

    def close
      @channels.each do |_id, ch|
        ch.close
      end
      if cb = @on_close_callback
        cb.call self
      end
    end

    def on_close(&blk : Client -> Nil)
      @on_close_callback = blk
    end

    def to_json(json : JSON::Builder)
      {
        address: @socket.remote_address.to_s,
        channels: @channels.size,
      }.to_json(json)
    end

    private def send_loop
      loop do
        frame = @send_chan.receive
        print "<= ", frame.inspect, "\n"
        @socket.write frame.to_slice
      end
    rescue ex : IO::Error, Errno
      print "Client connection closed ", @socket.remote_address, "\n"
    ensure
      close
    end

    private def open_channel(frame)
      @channels[frame.channel] = Client::Channel.new(self)
      send AMQP::Channel::OpenOk.new(frame.channel)
    end

    private def close_channel(frame)
      if ch = @channels.delete(frame.channel)
        ch.close
      end
      send AMQP::Channel::CloseOk.new(frame.channel)
    end

    private def declare_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if e.type == frame.exchange_type &&
            e.durable == frame.durable &&
            e.auto_delete == frame.auto_delete &&
            e.internal == frame.internal &&
            e.arguments == frame.arguments
          send AMQP::Exchange::DeclareOk.new(frame.channel)
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
        send AMQP::Exchange::DeclareOk.new(frame.channel)
      end
    end

    private def declare_queue(frame)
      if frame.queue_name.empty?
        frame.queue_name = "amq.gen-#{SecureRandom.urlsafe_base64(24)}"
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.durable == frame.durable &&
            q.exclusive == frame.exclusive &&
            q.auto_delete == frame.auto_delete &&
            q.arguments == frame.arguments
          send AMQP::Queue::DeclareOk.new(frame.channel, q.name,
                                          q.message_count, q.consumer_count)
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
        send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
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
        send AMQP::Queue::BindOk.new(frame.channel)
      end
    end

    private def unbind_queue(frame)
      if @vhost.queues.has_key? frame.queue_name && @vhost.exchanges.has_key? frame.exchange_name
        @vhost.apply(frame)
        send AMQP::Queue::UnbindOk.new(frame.channel)
      else
        send AMQP::Channel::Close.new(frame.channel, 401_u16,
                                      "Queue or exchange does not exists",
                                      frame.class_id, frame.method_id)
      end
    end

    private def basic_get(frame)
      if q = @vhost.queues.fetch(frame.queue, nil)
        if msg = q.get
          send AMQP::Basic::GetOk.new(frame.channel, 1_u64, false, msg.exchange_name,
                                      msg.routing_key, 1_u32)
          send AMQP::HeaderFrame.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
          send AMQP::BodyFrame.new(frame.channel, msg.body.to_slice)
        else
          send AMQP::Basic::GetEmpty.new(frame.channel)
        end
      else
        reply_code = "NOT_FOUND - no queue '#{frame.queue}' in vhost '#{@vhost.name}'"
        send AMQP::Channel::Close.new(frame.channel, 404_u16, reply_code, frame.class_id, frame.method_id)
      end
    end

    private def read_loop
      loop do
        frame = AMQP::Frame.decode @socket
        print "=> ", frame.inspect, "\n"
        case frame
        when AMQP::Connection::Close
          @socket.write AMQP::Connection::CloseOk.new.to_slice
          @socket.close
          close
          break
        when AMQP::Channel::Open
          open_channel(frame)
        when AMQP::Channel::Close
          close_channel(frame)
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
        when AMQP::Basic::Publish
          @channels[frame.channel].start_publish(frame.exchange, frame.routing_key)
        when AMQP::HeaderFrame
          @channels[frame.channel].next_msg_headers(frame.body_size, frame.properties)
        when AMQP::BodyFrame
          @channels[frame.channel].add_content(frame.body)
        when AMQP::Basic::Consume
          @channels[frame.channel].consume(frame)
          send AMQP::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
        when AMQP::Basic::Get
          basic_get(frame)
        when AMQPServer::AMQP::HeartbeatFrame
          send AMQPServer::AMQP::HeartbeatFrame.new
        else print "[ERROR] Unhandled frame ", frame.inspect, "\n"
        end
      end
    rescue ex : IO::EOFError
      print "Client connection closed ", @socket.remote_address, "\n"
    ensure
      close
    end

    def deliver(channel : UInt16, consumer_tag : String, redelivered : Bool, msg : Message)
      send AMQP::Basic::Deliver.new(channel, consumer_tag, @delivery_tag += 1, redelivered,
                                    msg.exchange_name, msg.routing_key)
      send AMQP::HeaderFrame.new(channel, 60_u16, 0_u16, msg.size, msg.properties)
      # TODO: split body in FRAME_MAX sizes
      send AMQP::BodyFrame.new(channel, msg.body.to_slice)
    end

    private def send(frame : AMQP::Frame)
      @send_chan.send frame
    end
  end
end
