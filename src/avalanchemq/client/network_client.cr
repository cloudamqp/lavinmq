require "./client"

module AvalancheMQ
  class NetworkClient < Client
    getter user, max_frame_size, auth_service, remote_address

    @max_frame_size : UInt32
    @max_channels : UInt16
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
      @socket = (ssl_client || tcp_socket).not_nil!
      @remote_address = tcp_socket.remote_address
      @local_address = tcp_socket.local_address
      log = vhost.log.dup
      log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @max_channels = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      name = "#{@remote_address} -> #{@local_address}"
      super(name, vhost, log, start_ok.client_properties)
      spawn heartbeat_loop, name: "Client#heartbeat_loop #{@remote_address}"
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def self.start(tcp_socket, ssl_client, config, vhosts, users, log)
      socket = ssl_client.nil? ? tcp_socket : ssl_client
      remote_address = tcp_socket.remote_address
      proto = uninitialized UInt8[8]
      bytes = socket.read_fully(proto.to_slice)

      if proto != AMQP::PROTOCOL_START && proto != AMQP::PROTOCOL_START_ALT
        socket.write AMQP::PROTOCOL_START.to_slice
        socket.close
        return
      end

      start = AMQP::Connection::Start.new
      socket.write start.to_slice
      start_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::StartOk)

      username = password = ""
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        username = resp[1...i]
        password = resp[(i + 1)..-1]
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.size.to_u32)
        username = tbl["LOGIN"].as(String)
        password = tbl["PASSWORD"].as(String)
      else "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end

      user = users[username]?
      unless user && user.password == password
        log.warn "Access denied for #{remote_address} using username \"#{username}\""
        props = start_ok.client_properties
        capabilities = props["capabilities"]?.try &.as(Hash(String, AMQP::Field))
        if capabilities && capabilities["authentication_failure_close"].try &.as(Bool)
          socket.write AMQP::Connection::Close.new(530_u16, "NOT_ALLOWED",
            start_ok.class_id,
            start_ok.method_id).to_slice
          close_on_ok(socket, log)
        else
          socket.close
        end
        return
      end
      socket.write AMQP::Connection::Tune.new(channel_max: 0_u16,
        frame_max: 131072_u32,
        heartbeat: config["heartbeat"]).to_slice
      tune_ok = AMQP::Frame.decode(socket).as(AMQP::Connection::TuneOk)
      open = AMQP::Frame.decode(socket).as(AMQP::Connection::Open)
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write AMQP::Connection::OpenOk.new.to_slice
          return self.new(tcp_socket, ssl_client, vhost, user, tune_ok, start_ok)
        else
          log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
          reply_text = "NOT_ALLOWED - '#{username}' doesn't have access to '#{vhost.name}'"
          socket.write AMQP::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id).to_slice
          close_on_ok(socket, log)
        end
      else
        log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
        socket.write AMQP::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id).to_slice
        close_on_ok(socket, log)
      end
      nil
    rescue ex : AMQP::FrameDecodeError
      log.warn "#{ex.cause.inspect} while #{remote_address} tried to establish connection"
      nil
    rescue ex : Exception
      log.warn "#{ex.inspect} while #{remote_address} tried to establish connection"
      socket.try &.close unless socket.try &.closed?
      nil
    end

    def channel_name_prefix
      @remote_address.to_s
    end

    def to_json(json : JSON::Builder)
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @max_channels,
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
      }.to_json(json)
    end

    private def declare_exchange(frame)
      name = frame.exchange_name
      if e = @vhost.exchanges.fetch(name, nil)
        if frame.passive || e.match?(frame)
          unless frame.no_wait
            send AMQP::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_precondition_failed(frame, "Existing exchange declared with other arguments")
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
        send AMQP::Exchange::DeclareOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_exchange(frame)
      if e = @vhost.exchanges.fetch(frame.exchange_name, nil)
        if frame.exchange_name.starts_with? "amq."
          send_access_refused(frame, "Not allowed to use the amq. prefix")
          return
        elsif !@user.can_config?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
        else
          @vhost.apply(frame)
          send AMQP::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
        end
      else
        send AMQP::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def delete_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.if_unused && !q.consumer_count.zero?
          send_precondition_failed(frame, "In use")
        elsif frame.if_empty && !q.message_count.zero?
          send_precondition_failed(frame, "Not empty")
        elsif !@user.can_config?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have permissions to delete queue '#{frame.queue_name}'")
        else
          size = q.message_count
          q.delete
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive
          send AMQP::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
        end
      else
        send AMQP::Queue::DeleteOk.new(frame.channel, 0_u32) unless frame.no_wait
      end
    end

    private def declare_queue(frame)
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.passive || q.match?(frame)
          unless frame.no_wait
            send AMQP::Queue::DeclareOk.new(frame.channel, q.name,
              q.message_count, q.consumer_count)
          end
        else
          send_precondition_failed(frame, "Existing queue declared with other arguments")
        end
        q.last_get_time = Time.now.epoch_ms
      elsif frame.passive
        send_not_found(frame)
      elsif frame.queue_name =~ /^amq\.(rabbitmq|direct)\.reply-to/
        unless frame.no_wait
          consumer_count = direct_reply_channel.nil? ? 0_u32 : 1_u32
          send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, consumer_count)
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
          send AMQP::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
        end
      end
    end

    private def bind_queue(frame)
      if !@vhost.queues.has_key? frame.queue_name
        send_not_found frame, "Queue #{frame.queue_name} not found"
      elsif !@vhost.exchanges.has_key? frame.exchange_name
        send_not_found frame, "Exchange #{frame.exchange_name} not found"
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
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
      elsif !@user.can_read?(@vhost.name, frame.exchange_name)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
      elsif !@user.can_write?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
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
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Exchange::BindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def unbind_exchange(frame)
      if !@vhost.exchanges.has_key? frame.destination
        send_not_found frame, "Exchange #{frame.destination} doesn't exists"
      elsif !@vhost.exchanges.has_key? frame.source
        send_not_found frame, "Exchange #{frame.source} doesn't exists"
      elsif !@user.can_read?(@vhost.name, frame.source)
        send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
      elsif !@user.can_write?(@vhost.name, frame.destination)
        send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
      else
        @vhost.apply(frame)
        send AMQP::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
      end
    end

    private def purge_queue(frame)
      unless @user.can_read?(@vhost.name, frame.queue_name)
        send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        return
      end
      if q = @vhost.queues.fetch(frame.queue_name, nil)
        if q.exclusive && !exclusive_queues.includes? q
          send_resource_locked(frame, "Exclusive queue")
        else
          messages_purged = q.purge
          send AMQP::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
        end
      else
        send_not_found(frame, "Queue #{frame.queue_name} not found")
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
        frame = AMQP::Frame.decode @socket
        @log.debug { "Read #{frame.inspect}" }
        if (!@running && !frame.is_a?(AMQP::Connection::Close | AMQP::Connection::CloseOk))
          @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          next
        end
        ok = process_frame(frame)
        break unless ok
        Fiber.yield if (i += 1) % 1000 == 0
      end
    rescue ex : AMQP::NotImplemented
      @log.error { "#{ex} when reading from socket" }
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

    private def close_socket
      @socket.close
    end

    def send(frame : AMQP::Frame)
      @log.debug { "Send #{frame.inspect}" }
      @socket.write frame.to_slice
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
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex})" }
      @socket.close
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    def deliver(frame, msg)
      @log.debug { "Merging delivery, header and body frame to one" }
      size = msg.size + 256
      buff = AMQP::MemoryIO.new(size)
      frame.encode buff
      header = AMQP::HeaderFrame.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
      header.encode(buff)
      pos = 0
      while pos < msg.size
        length = [msg.size - pos, @max_frame_size - 8].min
        body_part = msg.body[pos, length]
        @log.debug { "Sending BodyFrame (pos #{pos}, length #{length})" }
        AMQP::BodyFrame.new(frame.channel, body_part).encode(buff)
        pos += length
      end
      @socket.write buff.to_slice
      true
    rescue ex : IO::Error | Errno
      @log.info { "Lost connection, while sending (#{ex})" }
      cleanup
      false
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex})" }
      @socket.close
      cleanup
      false
    end

    private def heartbeat_loop
      return if @heartbeat == 0
      @log.debug { "Starting heartbeat loop with #{@heartbeat}s interval" }
      loop do
        sleep @heartbeat
        break unless @running
        send(AMQP::HeartbeatFrame.new) || break
      end
    end
  end
end
