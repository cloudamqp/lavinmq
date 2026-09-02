require "./consts"
require "./exchange"
require "./session"
require "./subscription_key"
require "./subscription_details"

module LavinMQ
  module MQTT
    # Owns a vhost's MQTT definitions: its sessions (one per client_id that has
    # subscribed) and their subscriptions, the latter held in the subscription
    # tree of the vhost's `MQTT::Exchange`.
    #
    # The exchange is created with the store rather than declared, so that it's
    # always there for a subscription to be made against, and it is never
    # persisted.
    #
    # Sessions and subscriptions are still *persisted* as AMQP `Queue::Declare`
    # and `Queue::Bind` frames in definitions.amqp: `LavinMQ::DefinitionsStore`
    # owns that file and calls in here for the in-memory state, both when
    # replaying frames on boot and when applying them at runtime. Only the
    # ownership of the state is split out so far — giving MQTT its own on-disk
    # format, and with it the migration of MQTT frames out of definitions.amqp,
    # is a separate step.
    class DefinitionsStore
      getter exchange : MQTT::Exchange

      def initialize(@vhost : VHost)
        @sessions = Hash(String, Session).new
        @exchange = MQTT::Exchange.new(@vhost, EXCHANGE)
      end

      # Session accessors

      def session?(name : String) : Session?
        @sessions[name]?
      end

      def session(name : String) : Session
        @sessions[name]
      end

      def session_exists?(name : String) : Bool
        @sessions.has_key?(name)
      end

      def each_session(& : Session ->) : Nil
        @sessions.each_value { |s| yield s }
      end

      def sessions : Array(Session)
        @sessions.values
      end

      def sessions_size : Int32
        @sessions.size
      end

      def sessions_clear : Nil
        @sessions.clear
      end

      # Creates a session, or returns nil if one already exists under that name,
      # so a caller can tell a fresh declaration from a no-op.
      def declare_session(name : String, clean_session : Bool) : Session?
        return if @sessions.has_key?(name)
        @sessions[name] = Session.new(@vhost, name, clean_session)
      end

      # Removes a session and all of its subscriptions, or returns nil if there
      # is no session under that name. The `Session` itself is not closed or
      # deleted — that's the caller's job, as it is for a queue.
      def delete_session(name : String) : Session?
        return unless session = @sessions.delete(name)
        # Collected first: unsubscribing mutates the tree we'd be iterating.
        topic_filters = Array(String).new
        each_subscription do |s, topic_filter, _qos|
          topic_filters << topic_filter if s.same?(session)
        end
        topic_filters.each { |tf| @exchange.unsubscribe(session, tf) }
        session
      end

      # Subscription accessors

      def subscribe(session : Session, topic_filter : String, qos : UInt8) : Bool
        @exchange.subscribe(session, topic_filter, qos)
      end

      def unsubscribe(session : Session, topic_filter : String) : Bool
        @exchange.unsubscribe(session, topic_filter)
      end

      def each_subscription(&block : (Session, String, UInt8) ->) : Nil
        @exchange.each_subscription(&block)
      end

      # One session's subscriptions, in the binding-details shape that the HTTP
      # API and the session itself read subscriptions through.
      def subscriptions(session : Session) : Array(SubscriptionDetails)
        result = Array(SubscriptionDetails).new
        each_subscription do |s, topic_filter, qos|
          next unless s.same?(session)
          result << SubscriptionDetails.new(@exchange.name, @vhost.name,
            SubscriptionKey.new(topic_filter, qos), s)
        end
        result
      end
    end
  end
end
