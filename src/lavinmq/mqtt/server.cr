require "../protocol_server"
require "./connection_factory"

module LavinMQ
  module MQTT
    class Server < ProtocolServer
      @brokers : Brokers
      @connection_factory : ConnectionFactory

      def initialize(server : LavinMQ::Server, config : Config = Config.instance)
        super(server, config, :mqtt)
        @brokers = Brokers.new(@server.vhosts)
        @connection_factory = ConnectionFactory.new(@server.authenticator, @brokers, @config)
      end

      def bind_tcp(bind : String = "::", port : Int = 1883)
        super(bind, port)
      end

      def brokers : Brokers
        @brokers
      end

      def broker(vhost : String) : Broker
        @brokers.broker(vhost)
      end

      def handle_connection(socket, connection_info)
        client = @connection_factory.start(socket, connection_info)
        socket.close if client.nil?
      end

      def close
        super
        @brokers.close
      end
    end
  end
end
