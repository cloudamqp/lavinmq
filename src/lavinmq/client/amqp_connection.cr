require "log"
require "../version"
require "./amqp/client"

module LavinMQ
  class AMQPConnection
    Log = ::Log.for "AMQPConnection"

    def self.start(socket, connection_info, vhosts, users) : Client?
      remote_address = connection_info.src
      Log.context.set(address: remote_address.to_s)
      socket.read_timeout = 15.seconds
      if confirm_header(socket)
        if start_ok = start(socket)
          if user = authenticate(socket, remote_address, users, start_ok)
            if tune_ok = tune(socket)
              if vhost = open(socket, vhosts, user)
                socket.read_timeout = heartbeat_timeout(tune_ok)
                return LavinMQ::AMQP::Client.new(socket, connection_info, vhost, user, tune_ok, start_ok)
              end
            end
          end
        end
      end
    rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      Log.warn { "#{ex} when #{remote_address} tried to establish connection" }
      nil
    rescue ex
      Log.error(exception: ex) { "Error while #{remote_address} tried to establish connection" }
      nil
    end

    private def self.heartbeat_timeout(tune_ok)
      if tune_ok.heartbeat > 0
        (tune_ok.heartbeat / 2).seconds
      end
    end

    def self.confirm_header(socket) : Bool
      proto = uninitialized UInt8[8]
      count = socket.read(proto.to_slice)
      if count.zero? # EOF, socket closed by peer
        false
      elsif proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        Log.warn { "Unexpected protocol '#{String.new(proto.to_slice)}', closing socket" }
        false
      else
        true
      end
    end

    SERVER_PROPERTIES = AMQP::Table.new({
      "product":      "LavinMQ",
      "platform":     "Crystal #{Crystal::VERSION}",
      "version":      LavinMQ::VERSION,
      "capabilities": {
        "publisher_confirms":           true,
        "exchange_exchange_bindings":   true,
        "basic.nack":                   true,
        "consumer_cancel_notify":       true,
        "connection.blocked":           true,
        "consumer_priorities":          true,
        "authentication_failure_close": true,
        "per_consumer_qos":             true,
        "direct_reply_to":              true,
      },
    })

    def self.start(socket)
      start = AMQP::Frame::Connection::Start.new(server_properties: SERVER_PROPERTIES)
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }
      if start_ok.bytesize > 4096
        Log.warn { "StartOk frame was #{start_ok.bytesize} bytes, max allowed is 4096 bytes" }
        return
      end
      start_ok
    end

    def self.credentials(start_ok)
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        if i = resp.index('\u0000', 1)
          {resp[1...i], resp[(i + 1)..-1]}
        else
          raise "Invalid authentication response"
        end
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian, io.bytesize.to_u32)
        {tbl["LOGIN"].as(String), tbl["PASSWORD"].as(String)}
      else raise "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end
    end

    def self.authenticate(socket, remote_address, users, start_ok)
      username, password = credentials(start_ok)
      user = users[username]?
      return user if user && user.password && user.password.not_nil!.verify(password) &&
                     guest_only_loopback?(remote_address, user)

      if user.nil?
        Log.warn { "User \"#{username}\" not found" }
      else
        Log.warn { "Authentication failure for user \"#{username}\"" }
      end
      props = start_ok.client_properties
      if capabilities = props["capabilities"]?.try &.as?(AMQP::Table)
        if capabilities["authentication_failure_close"]?.try &.as?(Bool)
          socket.write_bytes AMQP::Frame::Connection::Close.new(403_u16, "ACCESS_REFUSED",
            start_ok.class_id,
            start_ok.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
        end
      end
      nil
    end

    def self.tune(socket)
      socket.write_bytes AMQP::Frame::Connection::Tune.new(
        channel_max: Config.instance.channel_max,
        frame_max: Config.instance.frame_max,
        heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      tune_ok = AMQP::Frame.from_io(socket) do |frame|
        case frame
        when AMQP::Frame::Connection::TuneOk
          if frame.frame_max < 4096
            Log.warn { "Suggested Frame max (#{frame.frame_max}) too low, closing connection" }
            return
          end
          frame
        else
          Log.warn { "Expected TuneOk Frame got #{frame.inspect}" }
          return
        end
      end
      if tune_ok.frame_max < 4096
        Log.warn { "Suggested Frame max (#{tune_ok.frame_max}) too low, closing connection" }
        return
      end
      tune_ok
    end

    def self.open(socket, vhosts, user)
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      vhost_name = open.vhost.empty? ? "/" : open.vhost
      if vhost = vhosts[vhost_name]?
        if user.permissions[vhost_name]?
          if vhost.max_connections.try { |max| vhost.connections.size >= max }
            Log.warn { "Max connections (#{vhost.max_connections}) reached for vhost #{vhost_name}" }
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
          Log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
          reply_text = "NOT_ALLOWED - '#{user.name}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
        end
      else
        Log.warn { "VHost \"#{vhost_name}\" not found" }
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
      end
      nil
    end

    private def self.guest_only_loopback?(remote_address, user) : Bool
      return true unless user.name == "guest"
      return true unless Config.instance.guest_only_loopback?
      remote_address.loopback?
    end
  end
end
