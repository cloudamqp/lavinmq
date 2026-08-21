require "./session"
require "../vhost"
require "./permission_service"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost, @permission_service : PermissionService)
        # Sessions restored from disk are created before any broker exists, so
        # hand them the service now.
        @vhost.each_session { |session| session.permission_service = @permission_service }
      end

      def []?(client_id : String) : Session?
        @vhost.session?("#{SESSION_PREFIX}#{client_id}")
      end

      def [](client_id : String) : Session
        @vhost.session("#{SESSION_PREFIX}#{client_id}")
      end

      def declare(client : Client)
        self[client.client_id]? || begin
          @vhost.declare_queue("#{SESSION_PREFIX}#{client.client_id}", !client.@clean_session, client.@clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          session = self[client.client_id]
          session.permission_service = @permission_service
          session.client = client
          session
        end
      end
    end
  end
end
