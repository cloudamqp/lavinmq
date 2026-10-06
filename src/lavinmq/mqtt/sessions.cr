require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      def initialize(@vhost : VHost)
      end

      # A session stays in the vhost until its delete finishes, which can wait
      # for the definitions lock, so a reconnecting client waits it out rather
      # than getting the dying session
      def []?(client_id : String) : Session?
        name = "#{SESSION_PREFIX}#{client_id}"
        session = @vhost.mqtt.session?(name) || return
        return session unless session.deleting?
        session.wait_for_deletion
        @vhost.mqtt.session?(name).try { |s| s unless s.deleting? }
      end

      def [](client_id : String) : Session
        @vhost.mqtt.session("#{SESSION_PREFIX}#{client_id}")
      end

      # Returns nil if creating the session would exceed the vhost's max-queues
      # limit. An existing session is always returned, reusing one consumes no
      # new resource.
      def declare(client : Client) : Session?
        self[client.client_id]? || begin
          return if @vhost.queue_limit_reached?
          @vhost.declare_queue("#{SESSION_PREFIX}#{client.client_id}", !client.@clean_session, client.@clean_session, AMQP::Table.new({"x-queue-type": "mqtt"}))
          session = self[client.client_id]
          session.client = client
          session
        end
      end
    end
  end
end
