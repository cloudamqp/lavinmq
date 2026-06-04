require "./protocol"

module LavinMQ
  module MQTT
    # Used by a DR relay node, which must not serve application clients. It reads
    # the client's CONNECT and replies CONNACK with return code ServerUnavailable
    # (0x03), so MQTT clients get a clean rejection instead of a dropped socket.
    def self.reject_relay(socket, max_packet_size) : Nil
      socket.read_timeout = 15.seconds
      io = MQTT::IO.new(socket, max_packet_size)
      if io.read_packet.as?(Connect)
        Connack.new(false, Connack::ReturnCode::ServerUnavailable).to_io(io)
        io.flush
      end
    rescue ::IO::Error | ::MQTT::Protocol::Error
      # client disconnected or sent an invalid packet; nothing to do
    ensure
      socket.close rescue nil
    end
  end
end
