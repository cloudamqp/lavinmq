require "socket"
require "./protocol"
require "log"
require "./client"
require "../vhost"
require "../user"

module LavinMQ
  module MQTT
    class ConnectionFactory
      def initialize(@users : UserStore,
                     @vhost : VHost)
      end

      def start(socket : ::IO, connection_info : ConnectionInfo)
        io = ::MQTT::Protocol::IO.new(socket)
        if packet = MQTT::Packet.from_io(socket).as?(MQTT::Connect)
          Log.trace { "recv #{packet.inspect}" }
          if user = authenticate(io, packet)
            ::MQTT::Protocol::Connack.new(false, ::MQTT::Protocol::Connack::ReturnCode::Accepted).to_io(io)
            io.flush
            return LavinMQ::MQTT::Client.new(socket, connection_info, @vhost, user, packet.client_id, packet.clean_session?)
          end
        end
      rescue ex
        Log.warn { "Recieved the wrong packet" }
        socket.close
      end

      def authenticate(io, packet)
        return nil unless (username = packet.username) && (password = packet.password)
        user = @users[username]?
        return user if user && user.password && user.password.not_nil!.verify(String.new(password))
        # probably not good to differentiate between user not found and wrong password
        if user.nil?
          Log.warn { "User \"#{username}\" not found" }
        else
          Log.warn { "Authentication failure for user \"#{username}\"" }
        end
        ::MQTT::Protocol::Connack.new(false, ::MQTT::Protocol::Connack::ReturnCode::NotAuthorized).to_io(io)
        nil
      end
    end
  end
end
