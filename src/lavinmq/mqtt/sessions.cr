require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.fetch_queue("mqtt.#{client_id}").as(Session?)
      end

      def [](client_id : String) : Session
        @vhost.fetch_queue("mqtt.#{client_id}").as(Session)
      end

      def declare(client : Client)
        self[client.client_id]? || begin
          @vhost.declare_queue("mqtt.#{client.client_id}", !client.@clean_session, client.@clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          self[client.client_id].client = client
          self[client.client_id]
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
