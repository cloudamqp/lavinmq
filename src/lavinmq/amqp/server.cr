require "../protocol_server"
require "./connection_factory"

module LavinMQ
  module AMQP
    class Server < ProtocolServer
      @connection_factory : ConnectionFactory

      def initialize(server : LavinMQ::Server, config : Config = Config.instance)
        super(server, config, LavinMQ::Protocol::AMQP)
        @connection_factory = ConnectionFactory.new(@server.authenticator, @server.vhosts)
      end

      def bind_tcp(bind : String = "::", port : Int = 5672)
        super(bind, port)
      end

      def url
        addr = @listeners
          .select(TCPServer)
          .first
          .local_address
        "amqp://#{addr}"
      end

      def handle_connection(socket, connection_info)
        client = @connection_factory.start(socket, connection_info)
        socket.close if client.nil?
      end
    end
  end
end
