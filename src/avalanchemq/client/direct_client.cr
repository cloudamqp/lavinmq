require "./client"

module AvalancheMQ
  abstract class DirectClient < Client
    abstract def handle_frame(frame : Frame)

    def initialize(vhost : VHost, client_properties : Hash(String, AMQP::Field))
      log = vhost.log.dup
      log.progname += " direct=#{self.hash}"
      name = "localhost:#{self.hash}"
      vhost.add_connection(self)
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
        state:             @running ? "running" : "closed",
      }.to_json(json)
    end

    def channel_name_prefix
      @name
    end

    private def cleanup
      # noop
    end

    private def declare_exchange(frame)
      name = frame.exchange_name
      if e = @vhost.exchanges.fetch(name, nil)
        if e.match?(frame)
          send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
        else
          send_precondition_failed(frame, "Existing exchange '#{name}' declared with other arguments")
        end
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
      end
    end

    private def delete_exchange(frame)
      if @vhost.exchanges.has_key? frame.exchange_name
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeleteOk.new(frame.channel)
      else
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      end
    end

    private def delete_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        elsif frame.if_unused && !q.consumer_count.zero?
          send_precondition_failed(frame, "Queue '#{q.name}' in use")
        elsif frame.if_empty && !q.message_count.zero?
          send_precondition_failed(frame, "Queue '#{q.name}' is not empty")
        else
          size = q.message_count
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive
          send AMQP::Frame::Queue::DeleteOk.new(frame.channel, size)
        end
      else
        send AMQP::Frame::Queue::DeleteOk.new(frame.channel, 0_u32)
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !@exclusive_queues.includes? q
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        elsif q.match?(frame)
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, q.name, q.message_count, q.consumer_count)
        else
          send_precondition_failed(frame, "Existing queue '#{q.name}' declared with other arguments")
        end
      else
        if frame.queue_name.empty?
          frame.queue_name = Queue.generate_name
        end
        @vhost.apply(frame)
        if frame.exclusive
          @exclusive_queues << @vhost.queues[frame.queue_name]
        end
        send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::BindOk.new(frame.channel)
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
      end
    end

    private def bind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::BindOk.new(frame.channel)
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel)
      end
    end

    private def purge_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        else
          messages_purged = q.purge
          send AMQP::Frame::Queue::PurgeOk.new(frame.channel, messages_purged)
        end
      else
        send_not_found(frame, "Queue '#{frame.queue_name}' not found")
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

    private def ensure_open_channel(frame)
      return if @channels[frame.channel]?.try(&.running?)
      @channels[frame.channel] = Client::Channel.new(self, frame.channel)
    end

    def write(frame : AMQP::Frame)
      ensure_open_channel(frame)
      process_frame(frame)
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{ex} when reading handling frame" }
      if ex.channel > 0
        close_channel(ex, 540_u16, "Not implemented")
      else
        close_connection(ex, 540_u16, "Not implemented")
      end
    rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
      @log.info "Lost connection, while reading (#{ex.cause})" unless closed?
      cleanup
    rescue ex : Exception
      @log.error { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
      @running = false
    end

    def send(frame : AMQP::Frame)
      return false if closed?
      @send_oct_count += frame.bytesize + 8
      @log.debug { "Send #{frame.inspect}" }
      handle_frame(frame)
      case frame
      when AMQP::Frame::Connection::CloseOk
        @log.info "Disconnected"
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
