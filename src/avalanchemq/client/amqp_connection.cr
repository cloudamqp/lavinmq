module AvalancheMQ
  module AMQPConnection
    def self.start(socket, remote_address, local_address, vhosts, users, log)
      log = log.dup
      log.progname += " client=#{remote_address}"
      socket.read_timeout = 15
      confirm_header(socket, log) || return
      start_ok = start(socket)
      creds = credentials(start_ok)
      user = authenticate(socket, users, creds[:username], creds[:password], start_ok, log) || return
      tune_ok = tune(socket)
      if vhost = open(socket, vhosts, user, log)
        NetworkClient.new(socket, remote_address, local_address, vhost, user, tune_ok, start_ok)
      else
        nil
      end
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      log.warn "#{(ex.cause || ex).inspect} while #{remote_address} tried to establish connection"
      nil
    rescue ex : Exception
      log.error "Error while #{remote_address} tried to establish connection #{ex.inspect_with_backtrace}"
      socket.try &.close unless socket.try &.closed?
      nil
    ensure
      socket.read_timeout = nil
    end

    def self.confirm_header(socket, log)
      proto = uninitialized UInt8[8]
      socket.read(proto.to_slice)
      if proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        socket.close
        log.debug { "Unknown protocol #{proto}, closing socket" }
        return false
      end
      true
    end

    def self.start(socket)
      start = AMQP::Frame::Connection::Start.new
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
    end

    def self.credentials(start_ok)
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        { username: resp[1...i], password: resp[(i + 1)..-1] }
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian)
        { username: tbl["LOGIN"].as(String), password: tbl["PASSWORD"].as(String) }
      else raise "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end
    end

    def self.authenticate(socket, users, username, password, start_ok, log)
      user = users[username]?
      return user if user && user.password == password

      log.warn "User \"#{username}\" not found"
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
      nil
    end

    def self.tune(socket)
      socket.write_bytes AMQP::Frame::Connection::Tune.new(channel_max: 0_u16,
        frame_max: 131072_u32,
        heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::TuneOk) }
    end

    def self.open(socket, vhosts, user, log)
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return vhost
        else
          log.warn "Access denied for user \"#{user.name}\" to vhost \"#{open.vhost}\""
          reply_text = "NOT_ALLOWED - '#{user.name}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        end
      else
        log.warn "VHost \"#{open.vhost}\" not found"
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
          log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          if frame.is_a?(AMQP::Frame::Body)
            log.debug "Skipping body"
            frame.body.skip(frame.body_size)
          end
          frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk)
        end && break
      end
    rescue IO::EOFError
      log.debug { "Client closed socket without sending CloseOk" }
    rescue ex : IO::Error | Errno | AMQP::Error::FrameDecode
      log.warn { "#{ex.inspect} when waiting for CloseOk" }
    ensure
      socket.close
    end
  end
end
