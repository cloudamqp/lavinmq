module LavinMQ
  module MQTT
    class Session < Queue
      def initialize(@vhost : VHost, @name : String, @exclusive = true, @auto_delete = false, arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super
      end

      #if sub comes in with clean_session, set auto_delete on session
      #rm_consumer override for clean_session
    end
  end
end
