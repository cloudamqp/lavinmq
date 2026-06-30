require "../protocol_server"
require "./connection_factory"

module LavinMQ
  module MQTT
    class Server < ProtocolServer
      Log = LavinMQ::Log.for "mqtt.server"

      @brokers : Brokers
      @connection_factory : ConnectionFactory

      def initialize(server : LavinMQ::Server, config : Config = Config.instance)
        super(server, config, LavinMQ::Protocol::MQTT)
        @brokers = Brokers.new(@server.vhosts, @server.permission_groups)
        @connection_factory = ConnectionFactory.new(@server.authenticator, @brokers, @config)
        if @config.mqtt_topic_permissions_enabled? && @server.permission_groups.values.empty?
          Log.warn { "MQTT topic permissions are enabled but no permission groups are defined; " \
                     "all MQTT publish and subscribe will be denied until groups are configured" }
        end
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
