require "./client"

module AvalancheMQ
  class DirectClient < Client
    def initialize(@socket : ::Channel::Buffered(AMQP::Frame), vhost : VHost,
                   client_properties : Hash(String, AMQP::Field))
      log = vhost.log.dup
      log.progname += " direct"
      name = "localhost:#{self.hash}"
      super(name, vhost, log, client_properties)
    end

    def to_json(json : JSON::Builder)
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "direct",
        client_properties: @client_properties,
        vhost:             @vhost.name,
        protocol:          "Direct 0-9-1",
        name:              @name,
        state:             @socket.closed? ? "closed" : "running",
      }.to_json(json)
    end

    def channel_name_prefix
      @name
    end

    private def declare_exchange(frame)
      name = frame.exchange_name
      if e = @vhost.exchanges.fetch(name, nil)
        if e.match?(frame)
          send AMQP::Exchange::DeclareOk.new(frame.channel)
        else
          send_precondition_failed(frame, "Existing exchange declared with other arguments")
        end
      else
        @vhost.apply(frame)
        send AMQP::Exchange::DeclareOk.new(frame.channel)
      end
    end

    private def delete_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        @vhost.apply(frame)
      end
      send AMQP::Exchange::DeleteOk.new(frame.channel)
    end

    private def delete_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.if_unused && !q.consumer_count.zero?
          send_precondition_failed(frame, "In use")
        elsif frame.if_empty && !q.message_count.zero?
          send_precondition_failed(frame, "Not empty")
        else
          size = q.message_count
          q.delete
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive
          send AMQP::Queue::DeleteOk.new(frame.channel, size)
        end
      else
        send AMQP::Queue::DeleteOk.new(frame.channel, 0_u32)
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif q.match?(frame)
          send AMQP::Queue::DeclareOk.new(frame.channel, q.name, q.message_count, q.consumer_count)
        else
          send_precondition_failed(frame, "Existing queue declared with other arguments")
        end
      else
        if frame.queue_name.empty?
          frame.queue_name = Queue.generate_name
        end
        @vhost.apply(frame)
        if frame.exclusive
          @exclusive_queues << @vhost.queues[frame.queue_name]
        end
        send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      else
        @vhost.apply(frame)
        send AMQP::Queue::BindOk.new(frame.channel)
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      else
        @vhost.apply(frame)
        send AMQP::Queue::UnbindOk.new(frame.channel)
      end
    end

    private def bind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      else
        @vhost.apply(frame)
        send AMQP::Exchange::BindOk.new(frame.channel)
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      else
        @vhost.apply(frame)
        send AMQP::Exchange::UnbindOk.new(frame.channel)
      end
    end

    private def purge_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        else
          messages_purged = q.purge
          send AMQP::Queue::PurgeOk.new(frame.channel, messages_purged)
        end
      else
        send_not_found(frame, "Queue #{frame.queue_name} not found")
      end
    end

    private def start_publish(frame)
      with_channel frame, &.start_publish(frame)
    end

    private def consume(frame)
      with_channel frame, &.consume(frame)
    end

    private def basic_get(frame)
      with_channel frame, &.basic_get(frame)
    end

    private def close_socket
      @socket.close
    end

    private def ensure_open_channel(frame)
      return if @channels[frame.channel]?.try(&.running?)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
    end

    def write(frame : AMQP::Frame)
      return if @socket.closed?
      ensure_open_channel(frame)
      process_frame(frame)
    rescue ex : AMQP::NotImplemented
      @log.error { "#{ex} when reading handling frame" }
      if ex.channel > 0
        close_channel(ex, 540_u16, "Not implemented")
      else
        close_connection(ex, 540_u16, "Not implemented")
      end
    rescue ex : AMQP::FrameDecodeError
      @log.info "Lost connection, while reading (#{ex.cause})"
      cleanup
    rescue ex : Exception
      @log.error { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
      @running = false
    end

    def send(frame : AMQP::Frame)
      return if @socket.closed?
      ensure_open_channel(frame)
      @log.debug { "Send #{frame.inspect}" }
      @socket.send frame
      case frame
      when AMQP::Connection::CloseOk
        @log.info "Disconnected"
        @socket.close
        cleanup
        return false
      end
      true
    rescue ex : IO::Error | Errno
      @log.info { "Lost connection, while sending (#{ex})" }
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        name: @name,
      }
    end

    def deliver(channel, frame, msg)
      @log.debug { "Merging delivery, header and body frame to one" }
      header = AMQP::HeaderFrame.new(channel, 60_u16, 0_u16, msg.size, msg.properties)
      body = AMQP::BodyFrame.new(channel, msg.body)
      send(frame)
      send(header)
      send(body)
    end
  end
end
