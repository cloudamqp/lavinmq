require "socket"
require "./protocol"
require "log"
require "./client"
require "../vhost"
require "../user"
require "./broker"

module LavinMQ
  module MQTT
    class ConnectionFactory
      def initialize(@users : UserStore,
                     @vhost : VHost,
                     @broker : MQTT::Broker)
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        io = MQTT::IO.new(socket)
        if packet = MQTT::Packet.from_io(socket).as?(MQTT::Connect)
          Log.trace { "recv #{packet.inspect}" }
          if user = authenticate(io, packet)
            packet = assign_client_id_to_packet(packet) if packet.client_id.empty?
            session_present = @broker.session_present?(packet.client_id, packet.clean_session?)
            MQTT::Connack.new(session_present, MQTT::Connack::ReturnCode::Accepted).to_io(io)
            io.flush
            return @broker.connect_client(socket, connection_info, user, @vhost, packet)
          end
        end
      rescue ex : MQTT::Error::Connect
        Log.warn { "Connect error #{ex.inspect}" }
        if io
          MQTT::Connack.new(false, MQTT::Connack::ReturnCode.new(ex.return_code)).to_io(io)
        end
        socket.close
      rescue ex
        Log.warn { "Recieved invalid Connect packet" }
        socket.close
      end

      def authenticate(io, packet)
        return nil unless (username = packet.username) && (password = packet.password)
        user = @users[username]?
        return user if user && user.password && user.password.try(&.verify(String.new(password)))
        Log.warn { "Authentication failure for user \"#{username}\"" }
        MQTT::Connack.new(false, MQTT::Connack::ReturnCode::NotAuthorized).to_io(io)
        nil
      end

      def assign_client_id_to_packet(packet)
        client_id = Random::DEFAULT.base64(32)
        MQTT::Connect.new(client_id,
          packet.clean_session?,
          packet.keepalive,
          packet.username,
          packet.password,
          packet.will)
      end
    end
  end
end
