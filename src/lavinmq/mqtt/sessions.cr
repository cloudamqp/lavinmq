require "./session"
require "../vhost"

module LavinMQ
  module MQTT
    class Sessions
      class LimitReached < MQTT::Error; end

      # A queue that is not a session already has the session's name. AMQP and
      # the HTTP API refuse the prefix, but a definitions import does not.
      class NameTaken < MQTT::Error; end

      # With `permission_check_enabled`, the user may not write to the queue
      class NotPermitted < MQTT::Error; end

      def initialize(@vhost : VHost)
      end

      def []?(client_id : String) : Session?
        @vhost.session?("#{SESSION_PREFIX}#{client_id}")
      end

      # A connection's session. It becomes a queue only once it needs one,
      # see `#register`, so a publish-only client costs no queue.
      def build(client_id : String, clean_session : Bool) : Session
        Session.new(@vhost, "#{SESSION_PREFIX}#{client_id}", clean_session, registered: false)
      end

      # A queue that is not a session has the session's name
      def name_taken?(client_id : String) : Bool
        !@vhost.queue?("#{SESSION_PREFIX}#{client_id}").nil?
      end

      # Makes `session` a queue. Raises rather than returning false, so a
      # caller can tell the failures apart. A new durable session fsyncs the
      # definitions file, once: a reconnect finds it registered.
      def register(session : Session) : Nil
        return if session.registered?
        unless @vhost.register_session(session)
          raise NameTaken.new("queue \"#{session.name}\" in vhost \"#{@vhost.name}\" is not an MQTT session")
        end
        session.registered!
      end
    end
  end
end
