require "../client/connection_factory"
require "../auth/authenticator"
require "../auth/context"
require "../vhost_store"
require "../logger"
require "./client"
require "./session"

module LavinMQ::AMQP10
  class ConnectionFactory < LavinMQ::ConnectionFactory
    Log = LavinMQ::Log.for "amqp10.connection_factory"

    def initialize(@authenticator : Auth::Authenticator, @vhosts : VHostStore)
    end

    def start(socket, connection_info) : Client?
      metadata = ::Log::Metadata.build({address: connection_info.remote_address.to_s})
      start(socket, connection_info, Logger.new(Log, metadata))
    end

    def start(socket, connection_info, log : Logger) : Client?
      socket.write SASL_HEADER
      socket.flush

      user = authenticate(socket, connection_info, log) || return
      confirm_transport_header(socket, log) || return
      # Sized for our frame_max; the client keeps using it once the negotiated
      # (never larger) size is known, rather than allocating a second buffer.
      reader = FrameReader.new(socket, Config.instance.frame_max)
      open = read_open(reader, log) || return
      max_frame_size = negotiated_frame_max(open.max_frame_size)
      # Advertise our own idle-timeout so dead peers are reaped, and honor the
      # peer's so it does not drop us during idle periods.
      local_idle_timeout = server_idle_timeout
      remote_idle_timeout = open.idle_time_out
      vhost = resolve_vhost(socket, open, user, max_frame_size, local_idle_timeout, log) || return

      client = Client.new(socket, connection_info, vhost, user, "PLAIN", max_frame_size,
        remote_idle_timeout, local_idle_timeout, frame_reader: reader)
      client.send_open
      client
    rescue ex : IO::TimeoutError | IO::Error | OpenSSL::SSL::Error | DecodeError | ProtocolError
      log.warn { "#{ex} when #{connection_info.remote_address} tried to establish AMQP 1.0 connection" }
      nil
    rescue ex
      log.error(exception: ex) { "Error while #{connection_info.remote_address} tried to establish AMQP 1.0 connection" }
      nil
    end

    private def authenticate(socket, connection_info, log)
      send_sasl_mechanisms(socket)
      init = read_sasl_init(socket)
      unless init[0] == "PLAIN"
        send_sasl_outcome(socket, 1_u8)
        return
      end
      username, password = plain_credentials(init[1])
      context = Auth::Context.new(username, password, loopback: connection_info.remote_address.loopback?)
      if user = @authenticator.authenticate(context)
        send_sasl_outcome(socket, 0_u8)
        user
      else
        log.info { "Authentication failure for user \"#{username}\"" }
        send_sasl_outcome(socket, 1_u8)
        nil
      end
    end

    private def send_sasl_mechanisms(socket)
      fields = Array(Value).new(1)
      fields << Value.array([Value.symbol("PLAIN")])
      FrameWriter.write_performative(socket, 0_u16, SASL_FRAME_TYPE, Descriptor::SASL_MECHANISMS, fields)
    end

    private def read_sasl_init(socket) : Tuple(String, Bytes)
      frame = FrameReader.new(socket, MIN_MAX_FRAME_SIZE).read
      raise DecodeError.new("expected SASL frame") unless frame.type == SASL_FRAME_TYPE
      value = Codec.decode(frame.body_reader)
      described = value.described? || raise DecodeError.new("expected sasl-init")
      raise DecodeError.new("expected sasl-init") unless described.descriptor_code? == Descriptor::SASL_INIT
      fields = described.value.list? || raise DecodeError.new("sasl-init fields must be list")
      mechanism = fields[0]?.try(&.symbol?) || raise DecodeError.new("sasl-init missing mechanism")
      response = fields[1]?.try(&.binary?) || Bytes.empty
      {mechanism, response}
    end

    private def send_sasl_outcome(socket, code : UInt8)
      fields = Array(Value).new(1)
      fields << Value.ubyte(code)
      FrameWriter.write_performative(socket, 0_u16, SASL_FRAME_TYPE, Descriptor::SASL_OUTCOME, fields)
    end

    # SASL PLAIN (RFC 4616): authzid NUL authcid NUL passwd. Split on the raw
    # bytes; the response is opaque binary and need not be valid UTF-8.
    private def plain_credentials(response : Bytes) : Tuple(String, Bytes)
      first = response.index(0_u8) || raise DecodeError.new("invalid SASL PLAIN response")
      second = response.index(0_u8, first + 1) || raise DecodeError.new("invalid SASL PLAIN response")
      username = String.new(response[(first + 1)...second])
      password = response[(second + 1)..].dup
      {username, password}
    end

    private def confirm_transport_header(socket, log) : Bool
      header = uninitialized UInt8[8]
      socket.read_fully(header.to_slice)
      if header.to_slice == PROTOCOL_HEADER
        socket.write PROTOCOL_HEADER
        socket.flush
        true
      else
        log.warn { "AMQP 1.0 client did not send transport header after SASL" }
        false
      end
    rescue IO::EOFError
      log.warn { "AMQP 1.0 client did not send transport header after SASL" }
      false
    end

    private def read_open(reader : FrameReader, log) : Open?
      frame = reader.read
      raise DecodeError.new("expected AMQP frame") unless frame.type == AMQP_FRAME_TYPE
      open = Open.from_value(Codec.decode(frame.body_reader))
      open
    end

    private def resolve_vhost(socket, open : Open, user, max_frame_size : UInt32, idle_timeout : UInt32?, log)
      vhost_name = if hostname = open.hostname
                     hostname.starts_with?("vhost:") ? hostname[6..] : "/"
                   else
                     "/"
                   end
      if vhost = @vhosts[vhost_name]?
        if user.find_permission(vhost_name)
          if vhost.max_connections.try { |max| vhost.connections_size >= max }
            log.warn { "Max connections (#{vhost.max_connections}) reached for vhost #{vhost_name}" }
            refuse(socket, max_frame_size, idle_timeout, ErrorCondition::NOT_ALLOWED,
              "access to vhost '#{vhost_name}' refused: connection limit is reached")
            return
          end
          vhost
        else
          log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
          refuse(socket, max_frame_size, idle_timeout, ErrorCondition::UNAUTHORIZED_ACCESS,
            "'#{user.name}' does not have access to '#{vhost_name}'")
          nil
        end
      else
        log.warn { "VHost \"#{vhost_name}\" not found" }
        refuse(socket, max_frame_size, idle_timeout, ErrorCondition::NOT_FOUND, "vhost not found")
        nil
      end
    end

    # Open MUST be the first frame either peer sends (spec 2.4.1), so a refused
    # connection gets our Open followed by a Close carrying the error; clients
    # only surface the Close's error once they have seen the Open.
    private def refuse(socket, max_frame_size : UInt32, idle_timeout : UInt32?, condition, description)
      open = Open.new(Client::SERVER_CONTAINER_ID, nil, max_frame_size, idle_timeout)
      FrameWriter.write_frame_header(socket, open.frame_size, AMQP_FRAME_TYPE, 0_u16)
      open.write_body(socket)
      fields = Array(Value).new(1)
      fields << ErrorInfo.new(condition, description).to_value
      FrameWriter.write_performative(socket, 0_u16, AMQP_FRAME_TYPE, Descriptor::CLOSE, fields)
    end

    # Derive the AMQP 1.0 idle-timeout (milliseconds) from the configured
    # heartbeat, or nil to disable idle-timeout enforcement.
    private def server_idle_timeout : UInt32?
      heartbeat = Config.instance.heartbeat
      heartbeat.zero? ? nil : heartbeat.to_u32 * 1000
    end

    private def negotiated_frame_max(client_frame_max) : UInt32
      server = Config.instance.frame_max
      if client_frame_max.zero?
        server
      elsif server.zero?
        client_frame_max
      else
        Math.min(client_frame_max, server)
      end
    end
  end
end
