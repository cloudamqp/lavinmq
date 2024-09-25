module LavinMQ
  module MQTT
    class Session < Queue
       @clean_session : Bool = false
       getter clean_session
      def initialize(@vhost : VHost,
                     @name : String,
                     @auto_delete = false,
                     arguments : ::AMQ::Protocol::Table = AMQP::Table.new)
        super(@vhost, @name, false, @auto_delete, arguments)
      end

      def clean_session?
        @auto_delete
      end

      def durable?
        !clean_session?
      end

      #TODO: implement subscribers array and session_present? and send instead of false
      def connect(client)
        client.send(MQTT::Connack.new(false, MQTT::Connack::ReturnCode::Accepted))
      end
    end
  end
end
