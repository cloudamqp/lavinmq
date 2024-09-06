module LavinMQ
  module MQTT
    class Session < Queue
      def initialize(@vhost : VHost, @name : String, @exclusive = true, @auto_delete = false, arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super
      end

      #rm_consumer override for clean_session
    end
  end
end
