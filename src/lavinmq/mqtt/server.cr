require "../protocol_server"
require "./connection_factory"

module LavinMQ
  module MQTT
    class Server < ProtocolServer
      @connection_factory : ConnectionFactory

      def initialize(server : LavinMQ::Server, config : Config = Config.instance)
        super(server, config, LavinMQ::Protocol::MQTT)
        @connection_factory = ConnectionFactory.new(@server.authenticator, @server.vhosts, @config)
      end

      def bind_tcp(bind : String = "::", port : Int = 1883)
        super(bind, port)
      end

      def broker(vhost : String) : Broker
        @server.vhosts[vhost].mqtt_broker
      end

      def handle_connection(socket, connection_info)
        client = @connection_factory.start(socket, connection_info)
        socket.close if client.nil?
      end
    end
  end
end
