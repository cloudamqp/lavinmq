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
        "amqp://#{tcp_address}"
      end

      def handle_connection(socket, connection_info)
        if client = @connection_factory.create(socket, connection_info)
          vhost = client.vhost
          vhost.add_connection(client)
          begin
            client.run
          ensure
            vhost.rm_connection(client)
          end
        else
          socket.close
        end
      end
    end
  end
end
