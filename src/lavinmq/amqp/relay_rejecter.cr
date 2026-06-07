require "./connection_factory"
require "./connection_reply_code"

module LavinMQ
  module AMQP
    # Used by a DR relay node, which must not serve application clients. Instead
    # of leaving the AMQP port closed (raw connection-refused) it accepts the
    # connection and performs the minimal handshake — protocol header ->
    # Connection.Start -> Connection.Close(320) — so the client gets a clean,
    # explainable rejection ("server in relay mode") and disconnects.
    def self.reject_relay(socket) : Nil
      socket.read_timeout = 15.seconds if socket.responds_to?(:read_timeout=)
      proto = uninitialized UInt8[8]
      return if socket.read_fully?(proto.to_slice).nil? # EOF/short read, client went away
      return unless proto == AMQP::PROTOCOL_START_0_9_1 || proto == AMQP::PROTOCOL_START_0_9
      stream = AMQ::Protocol::Stream.new(socket)
      stream.write_bytes AMQP::Frame::Connection::Start.new(server_properties: ConnectionFactory::SERVER_PROPERTIES),
        ::IO::ByteFormat::NetworkEndian
      stream.flush
      begin
        stream.next_frame # read Start-Ok so the client is ready to receive the Close
      rescue IO::Error | AMQ::Protocol::Error
      end
      stream.write_bytes AMQP::Frame::Connection::Close.new(
        ConnectionReplyCode::CONNECTION_FORCED.value,
        "CONNECTION_FORCED - server in relay mode",
        0_u16, 0_u16),
        ::IO::ByteFormat::NetworkEndian
      stream.flush
    rescue IO::Error | AMQ::Protocol::Error
      # client disconnected mid-handshake; nothing to do
    ensure
      socket.close rescue nil
    end
  end
end
