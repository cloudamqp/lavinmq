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
      open = read_open(socket, log) || return
      vhost = resolve_vhost(socket, open, user, log) || return

      max_frame_size = negotiated_frame_max(open.max_frame_size)
      client = Client.new(socket, connection_info, vhost, user, "PLAIN", max_frame_size)
      client.send_open
      socket.read_timeout = nil
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
      context = Auth::Context.new(username, password.to_slice, loopback: connection_info.remote_address.loopback?)
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

    private def read_sasl_init(socket) : Tuple(String, String)
      frame = FrameReader.new(socket, MIN_MAX_FRAME_SIZE).read
      raise DecodeError.new("expected SASL frame") unless frame.type == SASL_FRAME_TYPE
      value = Codec.decode(frame.body_reader)
      described = value.described? || raise DecodeError.new("expected sasl-init")
      raise DecodeError.new("expected sasl-init") unless described.descriptor_code? == Descriptor::SASL_INIT
      fields = described.value.list? || raise DecodeError.new("sasl-init fields must be list")
      mechanism = fields[0]?.try(&.symbol?) || raise DecodeError.new("sasl-init missing mechanism")
      response = fields[1]?.try(&.binary?) || Bytes.empty
      {mechanism, String.new(response)}
    end

    private def send_sasl_outcome(socket, code : UInt8)
      fields = Array(Value).new(1)
      fields << Value.ubyte(code)
      FrameWriter.write_performative(socket, 0_u16, SASL_FRAME_TYPE, Descriptor::SASL_OUTCOME, fields)
    end

    private def plain_credentials(response : String) : Tuple(String, String)
      first = response.index('\0') || raise DecodeError.new("invalid SASL PLAIN response")
      second = response.index('\0', first + 1) || raise DecodeError.new("invalid SASL PLAIN response")
      {response[(first + 1)...second], response[(second + 1)..]}
    end

    private def confirm_transport_header(socket, log) : Bool
      header = uninitialized UInt8[8]
      count = socket.read(header.to_slice)
      if count == 8 && header.to_slice == PROTOCOL_HEADER
        true
      else
        log.warn { "AMQP 1.0 client did not send transport header after SASL" }
        false
      end
    end

    private def read_open(socket, log) : Open?
      frame = FrameReader.new(socket, Config.instance.frame_max).read
      raise DecodeError.new("expected AMQP frame") unless frame.type == AMQP_FRAME_TYPE
      open = Open.from_value(Codec.decode(frame.body_reader))
      open
    end

    private def resolve_vhost(socket, open : Open, user, log)
      vhost_name = if hostname = open.hostname
                     hostname.starts_with?("vhost:") ? hostname[6..] : "/"
                   else
                     "/"
                   end
      if vhost = @vhosts[vhost_name]?
        if user.find_permission(vhost_name)
          if vhost.max_connections.try { |max| vhost.connections_size >= max }
            log.warn { "Max connections (#{vhost.max_connections}) reached for vhost #{vhost_name}" }
            send_close(socket, ErrorCondition::NOT_ALLOWED,
              "access to vhost '#{vhost_name}' refused: connection limit is reached")
            return
          end
          vhost
        else
          log.warn { "Access denied for user \"#{user.name}\" to vhost \"#{vhost_name}\"" }
          send_close(socket, ErrorCondition::UNAUTHORIZED_ACCESS, "'#{user.name}' does not have access to '#{vhost_name}'")
          nil
        end
      else
        log.warn { "VHost \"#{vhost_name}\" not found" }
        send_close(socket, ErrorCondition::NOT_FOUND, "vhost not found")
        nil
      end
    end

    private def send_close(socket, condition, description)
      fields = Array(Value).new(1)
      fields << ErrorInfo.new(condition, description).to_value
      FrameWriter.write_performative(socket, 0_u16, AMQP_FRAME_TYPE, Descriptor::CLOSE, fields)
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
