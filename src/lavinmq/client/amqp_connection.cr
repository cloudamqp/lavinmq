module LavinMQ
  module AMQPConnection
    def self.start(socket, connection_info, vhosts, users)
      remote_address = connection_info.src
      log = Log.for "AMQPConnection[address=#{remote_address}]"
      socket.read_timeout = 15
      if confirm_header(socket, log)
        if start_ok = start(socket, log)
          if user = authenticate(socket, remote_address, users, start_ok, log)
            if tune_ok = tune(socket, log)
              if vhost = open(socket, vhosts, user, log)
                socket.read_timeout = heartbeat_timeout(tune_ok)
                Client.new(socket, connection_info, vhost, user, tune_ok, start_ok)
              end
            end
          end
        end
      end
    rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      Log.warn(exception: ex) { "error while #{remote_address} tried to establish connection" }
      nil
    rescue ex
      Log.error(exception: ex) { "Error while #{remote_address} tried to establish connection" }
      nil
    end

    private def self.heartbeat_timeout(tune_ok)
      if tune_ok.heartbeat > 0
        tune_ok.heartbeat / 2
      end
    end

    def self.confirm_header(socket, log) : Bool
      proto = uninitialized UInt8[8]
      count = socket.read(proto.to_slice)
      if count.zero? # EOF, socket closed by peer
        false
      elsif proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        log.warn { "Unexpected protocol '#{String.new(proto.to_slice)}', closing socket" }
        false
      else
        true
      end
    end

    SERVER_PROPERTIES = AMQP::Table.new({
      "product"      => "LavinMQ",
      "platform"     => "Crystal #{Crystal::VERSION}",
      "version"      => LavinMQ::VERSION,
      "capabilities" => AMQP::Table.new({
        "publisher_confirms"           => true,
        "exchange_exchange_bindings"   => true,
        "basic.nack"                   => true,
        "consumer_cancel_notify"       => true,
        "connection.blocked"           => true,
        "consumer_priorities"          => true,
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

    def self.authenticate(socket, remote_address, users, start_ok, log)
      username, password = credentials(start_ok)
      user = users[username]?
      return user if user && user.password && user.password.not_nil!.verify(password) &&
                     guest_only_loopback?(remote_address, user)

      if user.nil?
        log.warn { "User \"#{username}\" not found" }
      else
        log.warn { "Authentication failure for user \"#{username}\"" }
      end
      props = start_ok.client_properties
      capabilities = props["capabilities"]?.try &.as(AMQP::Table)
      if capabilities && capabilities["authentication_failure_close"]?.try &.as(Bool)
        socket.write_bytes AMQP::Frame::Connection::Close.new(403_u16, "ACCESS_REFUSED",
          start_ok.class_id,
          start_ok.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
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
            return
          end
          frame
        else
          log.warn { "Expected TuneOk Frame got #{frame.inspect}" }
          return
        end
      end
      if tune_ok.frame_max < 4096
        log.warn { "Suggested Frame max (#{tune_ok.frame_max}) too low, closing connection" }
        return
      end
      tune_ok
    end

    def self.open(socket, vhosts, user, log)
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      vhost_name = open.vhost.empty? ? "/" : open.vhost
      if vhost = vhosts[vhost_name]? || nil
        if user.permissions[vhost_name]? || nil
          if vhost.max_connections.try { |max| vhost.connections.size > max }
            log.warn { "Max connections (#{vhost.max_connections}) reached for vhost #{vhost_name}" }
            reply_text = "NOT_ALLOWED - access to vhost '#{vhost_name}' refused: connection limit (#{vhost.max_connections}) is reached"
            socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
              open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
            socket.flush
            return
          end
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return vhost
        else
          log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
          reply_text = "NOT_ALLOWED - '#{user.name}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
        end
      else
        log.warn { "VHost \"#{vhost_name}\" not found" }
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
      end
      nil
    end

    private def self.guest_only_loopback?(remote_address, user) : Bool
      return true unless user.name == "guest"
      return true unless Config.instance.guest_only_loopback
      remote_address.loopback?
    end
  end
end
