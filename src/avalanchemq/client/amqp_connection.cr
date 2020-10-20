module AvalancheMQ
  module AMQPConnection
    def self.start(socket, remote_address, local_address, vhosts, users, log, churn_events)
      log.progname += " client=#{remote_address}"
      socket.read_timeout = 15
      if confirm_header(socket, log)
        if start_ok = start(socket, log)
          if user = authenticate(socket, users, start_ok, log)
            if tune_ok = tune(socket, log)
              if vhost = open(socket, vhosts, user, log)
                Client.new(socket, remote_address, local_address, vhost, user, churn_events, tune_ok, start_ok)
              end
            end
          end
        end
      end
    rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      log.warn "#{(ex.cause || ex).inspect} while #{remote_address} tried to establish connection"
      socket.try &.close unless socket.try &.closed?
      nil
    rescue ex
      log.error "Error while #{remote_address} tried to establish connection #{ex.inspect_with_backtrace}"
      socket.try &.close unless socket.try &.closed?
      nil
    ensure
      timeout =
        if t = tune_ok
          if t.heartbeat > 0
            t.heartbeat / 2
          end
        end
      socket.read_timeout = timeout
    end

    def self.confirm_header(socket, log)
      proto = uninitialized UInt8[8]
      socket.read(proto.to_slice)
      if proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        socket.close
        log.warn { "Unexpected protocol '#{String.new(proto.to_slice)}', closing socket" }
        return false
      end
      true
    end

    SERVER_PROPERTIES = AMQP::Table.new({
      "product"      => "AvalancheMQ",
      "platform"     => "Crystal #{Crystal::VERSION}",
      "version"      => AvalancheMQ::VERSION,
      "capabilities" => AMQP::Table.new({
        "publisher_confirms"           => true,
        "exchange_exchange_bindings"   => true,
        "basic.nack"                   => true,
        "consumer_cancel_notify"       => true,
        "connection.blocked"           => true,
        "consumer_priorities"          => false,
        "authentication_failure_close" => true,
        "per_consumer_qos"             => true,
        "direct_reply_to"              => true,
      }),
    })

    def self.start(socket, log)
      start = AMQP::Frame::Connection::Start.new(server_properties: SERVER_PROPERTIES)
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
      if start_ok.bytesize > 4096
        log.warn { "StartOk frame was #{start_ok.bytesize} bytes, max allowed is 4096 bytes" }
        socket.close
        return
      end
      start_ok
    end

    def self.credentials(start_ok)
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        {resp[1...i], resp[(i + 1)..-1]}
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.bytesize.to_u32)
        {tbl["LOGIN"].as(String), tbl["PASSWORD"].as(String)}
      else raise "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end
    end

    def self.authenticate(socket, users, start_ok, log)
      username, password = credentials(start_ok)
      user = users[username]?
      return user if user && user.password && user.password.not_nil!.verify(password)

      if user.nil?
        log.warn "User \"#{username}\" not found"
      else
        log.warn "Authentication failure for user \"#{username}\""
      end
      props = start_ok.client_properties
      capabilities = props["capabilities"]?.try &.as(AMQP::Table)
      if capabilities && capabilities["authentication_failure_close"]?.try &.as(Bool)
        socket.write_bytes AMQP::Frame::Connection::Close.new(403_u16, "ACCESS_REFUSED",
          start_ok.class_id,
          start_ok.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket, log)
      else
        socket.close
      end
      nil
    end

    def self.tune(socket, log)
      socket.write_bytes AMQP::Frame::Connection::Tune.new(
        channel_max: Config.instance.channel_max,
        frame_max: Config.instance.frame_max,
        heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      tune_ok = AMQP::Frame.from_io(socket) do |frame|
        case frame
        when AMQP::Frame::Connection::TuneOk
          if frame.frame_max < 4096
            log.warn { "Suggested Frame max (#{frame.frame_max}) too low, closing connection" }
            socket.close
            return
          end
          frame
        else
          log.warn { "Expected TuneOk Frame got #{frame.inspect}" }
          socket.close
          return
        end
      end
      if tune_ok.frame_max < 4096
        log.warn { "Suggested Frame max (#{tune_ok.frame_max}) too low, closing connection" }
        socket.close
        return
      end
      tune_ok
    end

    def self.open(socket, vhosts, user, log)
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      vhost_name = open.vhost.empty? ? "/" : open.vhost
      if vhost = vhosts[vhost_name]? || nil
        if user.permissions[vhost_name]? || nil
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return vhost
        else
          log.warn "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\""
          reply_text = "NOT_ALLOWED - '#{user.name}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        end
      else
        log.warn "VHost \"#{vhost_name}\" not found"
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket, log)
      end
      nil
    end

    def self.close_on_ok(socket, log)
      loop do
        AMQP::Frame.from_io(socket, IO::ByteFormat::NetworkEndian) do |frame|
          if frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk)
            true
          else
            log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            if frame.is_a?(AMQP::Frame::Body)
              log.debug "Skipping body"
              frame.body.skip(frame.body_size)
            end
            false
          end
        end && break
      end
    rescue IO::EOFError
      log.debug { "Client closed socket without sending CloseOk" }
    rescue ex
      log.warn { "#{ex.inspect} while waiting for CloseOk" }
    ensure
      socket.close
    end
  end
end
