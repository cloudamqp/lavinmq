require "./client"

module AvalancheMQ
  class NetworkClient < Client
    getter user, max_frame_size, auth_mechanism, remote_address, heartbeat, channel_max

    @max_frame_size : UInt32
    @channel_max : UInt16
    @heartbeat : UInt16
    @auth_mechanism : String
    @remote_address : Socket::IPAddress
    @local_address : Socket::IPAddress
    @socket : TCPSocket | OpenSSL::SSL::Socket

    def initialize(tcp_socket : TCPSocket,
                   ssl_client : OpenSSL::SSL::Socket?,
                   vhost : VHost,
                   @user : User,
                   tune_ok,
                   start_ok)
      @socket = ssl_client || tcp_socket
      @remote_address = tcp_socket.remote_address
      @local_address = tcp_socket.local_address
      log = vhost.log.dup
      log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      name = "#{@remote_address} -> #{@local_address}"
      super(name, vhost, log, start_ok.client_properties)
      spawn heartbeat_loop, name: "Client#heartbeat_loop #{@remote_address}"
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def self.start(tcp_socket, ssl_client, vhosts, users, log)
      socket = ssl_client || tcp_socket
      remote_address = tcp_socket.remote_address
      proto = uninitialized UInt8[8]
      socket.read_fully(proto.to_slice)
      if proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        socket.close
        log.debug { "Unknown protocol #{proto}, closing socket" }
        return
      end

      start = AMQP::Frame::Connection::Start.new
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }

      username = password = ""
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        username = resp[1...i]
        password = resp[(i + 1)..-1]
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian)
        username = tbl["LOGIN"].as(String)
        password = tbl["PASSWORD"].as(String)
      else "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end

      user = users[username]?
      unless user && user.password == password
        log.warn "Access denied for #{remote_address} using username \"#{username}\""
        props = start_ok.client_properties
        capabilities = props["capabilities"]?.try &.as(Hash(String, AMQP::Field))
        if capabilities && capabilities["authentication_failure_close"]?.try &.as(Bool)
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED",
            start_ok.class_id,
            start_ok.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        else
          socket.close
        end
        return
      end
      socket.write_bytes AMQP::Frame::Connection::Tune.new(channel_max: 0_u16,
        frame_max: 131072_u32,
        heartbeat: Server.config.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      tune_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::TuneOk) }
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return self.new(tcp_socket, ssl_client, vhost, user, tune_ok, start_ok)
        else
          log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
          reply_text = "NOT_ALLOWED - '#{username}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        end
      else
        log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket, log)
      end
      nil
    rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
      log.warn "#{(ex.cause || ex).inspect} while #{remote_address} tried to establish connection"
      nil
    rescue ex : Exception
      log.error "Error while #{remote_address} tried to establish connection #{ex.inspect_with_backtrace}"
      socket.try &.close unless socket.try &.closed?
      nil
    end

    def channel_name_prefix
      @remote_address.to_s
    end

    private def cleanup
      @socket.close unless @socket.closed?
      super
    end

    def to_json(json : JSON::Builder)
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @channel_max,
        timeout:           @heartbeat,
        client_properties: @client_properties,
        vhost:             @vhost.name,
        user:              @user.name,
        protocol:          "AMQP 0-9-1",
        auth_mechanism:    @auth_mechanism,
        host:              @local_address.address,
        port:              @local_address.port,
        peer_host:         @remote_address.address,
        peer_port:         @remote_address.port,
        name:              @name,
        ssl:               @socket.is_a?(OpenSSL::SSL::Socket),
        state:             @socket.closed? ? "closed" : "running",
      }.merge(stats_details).to_json(json)
    end

    private def declare_exchange(frame)
      name = frame.exchange_name
      if e = @vhost.exchanges.fetch(name, nil)
        if frame.passive || e.match?(frame)
          unless frame.no_wait
            send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_precondition_failed(frame, "Existing exchange '#{name}' declared with other arguments")
        end
      elsif frame.passive
        send_not_found(frame)
      elsif name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        ae = frame.arguments["x-alternate-exchange"]?.try &.as?(String)
        ae_ok = ae.nil? || (@user.can_write?(@vhost.name, ae) && @user.can_read?(@vhost.name, name))
        unless @user.can_config?(@vhost.name, name) && ae_ok
          send_access_refused(frame, "User doesn't have permissions to declare exchange '#{name}'")
          return
        end
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::DeclareOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_exchange(frame)
      if @vhost.exchanges.has_key? frame.exchange_name
        if frame.exchange_name.starts_with? "amq."
          send_access_refused(frame, "Not allowed to use the amq. prefix")
          return
        elsif !@user.can_config?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
        else
          @vhost.apply(frame)
          send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
        end
      else
        send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
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
        elsif !@user.can_config?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have permissions to delete queue '#{q.name}'")
        else
          size = q.message_count
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive
          send AMQP::Frame::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
        end
      else
        send AMQP::Frame::Queue::DeleteOk.new(frame.channel, 0_u32) unless frame.no_wait
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.passive || q.match?(frame)
          unless frame.no_wait
            send AMQP::Frame::Queue::DeclareOk.new(frame.channel, q.name,
              q.message_count, q.consumer_count)
          end
        else
          send_precondition_failed(frame, "Existing queue '#{q.name}' declared with other arguments")
        end
        q.last_get_time = Time.now.to_unix_ms
      elsif frame.passive
        send_not_found(frame)
      elsif frame.queue_name =~ /^amq\.(rabbitmq|direct)\.reply-to/
        unless frame.no_wait
          consumer_count = direct_reply_channel.nil? ? 0_u32 : 1_u32
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, consumer_count)
        end
      elsif frame.queue_name.starts_with? "amq."
        send_access_refused(frame, "Not allowed to use the amq. prefix")
      else
        if frame.queue_name.empty?
          frame.queue_name = Queue.generate_name
        end
        dlx = frame.arguments["x-dead-letter-exchange"]?.try &.as?(String)
        dlx_ok = dlx.nil? || (@user.can_write?(@vhost.name, dlx) && @user.can_read?(@vhost.name, name))
        unless @user.can_config?(@vhost.name, frame.queue_name) && dlx_ok
          send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue_name}'")
          return
        end
        @vhost.apply(frame)
        if frame.exclusive
          @exclusive_queues << @vhost.queues[frame.queue_name]
        end
        unless frame.no_wait
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
        end
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Queue::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue '#{frame.queue_name}' not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
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
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Frame::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      unless @user.can_read?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        return
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        else
          messages_purged = q.purge
          send AMQP::Frame::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
        end
      else
        send_not_found(frame, "Queue '#{frame.queue_name}' not found")
      end
    end

    private def start_publish(frame)
      unless @user.can_write?(@vhost.name, frame.exchange)
        send_access_refused(frame, "User not allowed to publish to exchange '#{frame.exchange}'")
      end
      with_channel frame, &.start_publish(frame)
    end

    private def consume(frame)
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
      end
      with_channel frame, &.consume(frame)
    end

    private def basic_get(frame)
      unless @user.can_read?(@vhost.name, frame.queue)
        send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
      end
      with_channel frame, &.basic_get(frame)
    end

    private def read_loop
      i = 0
      loop do
        AMQP::Frame.from_io(@socket) do |frame|
          @log.debug { "Read #{frame.inspect}" }
          if (!@running && !frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk))
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            if frame.is_a?(AMQP::Frame::Body)
              @log.debug "Skipping body"
              frame.body.skip(frame.body_size)
            end
            next true
          end
          process_frame(frame)
        end || break
        Fiber.yield if (i += 1) % 1000 == 0
      end
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{ex} when reading from socket" }
      if ex.channel > 0
        close_channel(ex, 540_u16, "Not implemented")
      else
        close_connection(ex, 540_u16, "Not implemented")
      end
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      @log.info { "Lost connection, while reading (#{ex.inspect})" } unless closed?
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
      @write_lock.synchronize do
        @socket.write_bytes frame, IO::ByteFormat::NetworkEndian
        @socket.flush
      end
      case frame
      when AMQP::Frame::Connection::CloseOk
        @log.info "Disconnected"
        cleanup
        return false
      end
      true
    rescue ex : IO::Error | Errno
      @log.info { "Lost connection, while sending (#{ex.inspect})" } unless closed?
      cleanup
      false
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    @write_lock = Mutex.new

    def deliver(frame, msg)
      @write_lock.synchronize do
        @log.debug { "Sending #{frame.inspect}" }
        @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
        header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
        @log.debug { "Sending #{header.inspect}" }
        @socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
        pos = 0
        while pos < msg.size
          length = Math.min(msg.size - pos, @max_frame_size - 8).to_u32
          @log.debug { "Sending BodyFrame (pos #{pos}, length #{length})" }
          body = AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
          body.to_io(@socket, ::IO::ByteFormat::NetworkEndian)
          pos += length
        end
        @log.debug { "Flushing" }
        @socket.flush
      end
      true
    rescue ex : IO::Error | Errno
      @log.info { "Lost connection, while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Delivery exception: #{ex.inspect_with_backtrace}" }
      raise ex
    end

    private def heartbeat_loop
      return if @heartbeat == 0
      @log.debug { "Starting heartbeat loop with #{@heartbeat}s interval" }
      loop do
        sleep @heartbeat
        break unless @running
        send(AMQP::Frame::Heartbeat.new) || break
      end
    end
  end
end
