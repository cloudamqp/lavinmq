require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    # Just a wrapper around the vhost queues
    class Sessions
      QueueArguments = AMQP::Table.new({"x-queue-type": "mqtt"})

      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.queue("mqtt.#{client_id}").as(Session?)
      end

      def [](client_id : String) : Session
        @vhost.queue("mqtt.#{client_id}").as(Session)
      end

      def declare(client : Client) : Session
        if q = @vhost.queue("mqtt.#{client.client_id}")
          q.as(Session)
        else
          @vhost.declare_queue("mqtt.#{client.client_id}", !client.clean_session?, client.clean_session?, QueueArguments)
          session = @vhost.queue("mqtt.#{client.client_id}").as(Session)
          session.client = client
          session
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
