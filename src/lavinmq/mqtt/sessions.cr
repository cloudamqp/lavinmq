require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    struct Sessions
      @queues : Hash(String, Queue)

      def initialize(@vhost : VHost)
        @queues = @vhost.queues
      end

      def []?(client_id : String) : Session?
        @queues["mqtt.#{client_id}"]?.try &.as(Session)
      end

      def [](client_id : String) : Session
        @queues["mqtt.#{client_id}"].as(Session)
      end

      def declare(client_id : String, clean_session : Bool)
        self[client_id]? || begin
          @vhost.declare_queue("mqtt.#{client_id}", !clean_session, clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          self[client_id]
        end
      end

      def delete(client_id : String)
        @vhost.delete_queue("mqtt.#{client_id}")
      end

      def delete(session : Session)
        session.delete
      end
    end
  end
end
