module LavinMQ
  module MQTT
    class Session < Queue
      def initialize(@vhost : VHost,
                     @name : String,
                     @exclusive = true,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super
      end


      #TODO: implement subscribers array and session_present? and send instead of false
      def connect(client)
        client.send(MQTT::Connack.new(false, MQTT::Connack::ReturnCode::Accepted))
      end
    end
  end
end
