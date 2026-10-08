require "./client"
require "./consts"
require "./exchange"
require "./protocol"
require "./session"
require "./sessions"
require "./retain_store"
require "../vhost"

module LavinMQ
  module MQTT
    class Broker
      getter vhost
      private getter sessions

      # The `Broker` class acts as an intermediary between the `Server` and MQTT connections.
      # It is initialized by the `Server` and manages client connections, sessions, and message exchange.
      # Responsibilities include:
      # - Handling client connections and disconnections
      # - Managing client sessions (clean and persistent)
      # - Publishing messages to the exchange
      # - Subscribing and unsubscribing clients to/from topics
      # - Handling the retain store
      # - Interfacing with the virtual host (vhost) and the exchange to route messages
      # The `Broker` class helps keep the MQTT client concise and focused on the protocol.
      #
      # Connection lifecycle rules, which the takeover [MQTT-3.1.4-3] relies on:
      # 1. Registering, taking over and removing a client, and creating or
      #    deleting its session for it, happen under its client_id's lock.
      # 2. A registered client always reaches `run_client`'s `ensure`.
      # 3. `Client#close` waits for the read fiber only once `Client#run` began.
      # 4. A client lock is never taken while holding the definitions lock.
      def initialize(@vhost : VHost)
        @sessions = Sessions.new(@vhost)
        @clients = Hash(String, Client).new
        @client_locks = Hash(String, ClientLock).new
        @retain_store = RetainStore.new(File.join(@vhost.data_dir, "mqtt_retained_store"), @vhost.replicator, persister: @vhost.persister)
        @exchange = @vhost.mqtt_exchange
      end

      def permission_service : PermissionService
        @vhost.mqtt_permission_service
      end

      # v5 reads the property, absent meaning 0 (§3.1.2.11.2). The shard gives a
      # v3 CONNECT the same reading of its Clean Session bit: 1 ends the session
      # with the connection, 0 keeps it forever, as LavinMQ has always done.
      private def session_expiry_interval(packet : Protocol::Connect) : UInt32
        packet.properties.session_expiry_interval
      end

      # A reconnecting client_id displaces the existing connection in
      # `add_client`, so the connection count doesn't grow
      def connection_limit_reached?(client_id : String) : Bool
        return false if @clients.has_key?(client_id)
        @vhost.connection_limit_reached?
      end

      # Yields `session_present` for the caller to send CONNACK. The client is
      # registered before that, so a later CONNECT takes it over even
      # mid-CONNACK; it attaches to its session only in `Client#run`.
      def run_client(io, connection_info, user, packet, & : Bool ->) : Client
        client, session_present = add_client(io, connection_info, user, packet)
        begin
          yield session_present
          # No yield between this check and `Client#run` setting `@started`.
          if client.closed? || client.session.deleted?
            client.log.info { "Taken over or session deleted before attaching, closing" }
            client.force_close
            return client
          end
          client.run
        ensure
          remove_client(client)
        end
        client
      end

      # Every connection gets a session, not only one that subscribes: it holds
      # the inbound QoS 2 state too, and it is what makes a returning persistent
      # client's session present [MQTT-3.2.2-3]. Raises before anything is
      # sent, so the CONNECT can still be refused.
      private def add_client(io, connection_info, user, packet) : {Client, Bool}
        with_client_lock(packet.client_id) { add_client_locked(io, connection_info, user, packet) }
      end

      private def add_client_locked(io, connection_info, user, packet) : {Client, Bool}
        client_id = packet.client_id
        if prev_client = @clients[client_id]?
          prev_client.close(
            "New client #{connection_info.remote_address} " \
            "(username=#{packet.username}) connected as #{client_id}",
            Protocol::Disconnect::ReasonCode::SessionTakenOver)
          remove_client_locked(prev_client)
        end
        interval = session_expiry_interval(packet)
        existing = sessions[client_id]?
        # A clean session starts with no state at all [MQTT-3.1.2-4], and a
        # 0-interval session ends with its connection, which a takeover is
        # (3.1.4). Clean Start and the interval are separate inputs: the first
        # decides whether to discard, the second how long the session this
        # connection ends up with will outlive it.
        if existing && (packet.clean_start? || existing.auto_delete?)
          existing.delete
          existing = nil
        end
        session = begin
          sessions.declare(client_id, interval)
        rescue Sessions::LimitReached
          raise Protocol::Error::ServerUnavailable.new(
            "queue limit (#{@vhost.max_queues}) reached in vhost \"#{@vhost.name}\"")
        rescue ex : Sessions::NameTaken
          # Retrying cannot help until an operator removes that queue.
          raise Protocol::Error::IdentifierRejected.new(
            "queue \"#{ex.message}\" in vhost \"#{@vhost.name}\" is not an MQTT session")
        end
        # A resumed session adopts this connection's interval. Its expiry clock,
        # if running, captured the old one at disconnect and stops once
        # `session.resume` below claims it, so narrowing it here cannot expire
        # the session about to be resumed.
        session.session_expiry_interval = interval if existing
        client = MQTT::Client.new(io,
          connection_info,
          user,
          self,
          session,
          client_id: client_id,
          keepalive: packet.keep_alive,
          will: packet.will,
          max_packet_size: packet.properties.maximum_packet_size,
          receive_maximum: packet.properties.receive_maximum,
          session_expiry_interval: interval)
        @clients[client_id] = client
        @vhost.add_connection client
        # Here, not at attach, which waits for CONNACK: a connection opened
        # within the Will Delay Interval cancels the will [MQTT-3.1.3-9], and
        # the session must not expire after CONNACK says it is present. Last,
        # so nothing can raise between the claim and `remove_client` ending it.
        session.resume
        {client, !existing.nil?}
      end

      # One entry per client_id being connected or removed, so the map is empty
      # in between. `users` counts the holder and its waiters, a
      # record so the entry costs no allocation besides the `Mutex`. A `Hash`
      # never shrinks, so the map keeps the capacity of its largest burst.
      private record ClientLock, mutex : Mutex, users : Int32

      private def with_client_lock(client_id : String, &)
        lock = @client_locks[client_id]?
        lock = lock ? lock.copy_with(users: lock.users + 1) : ClientLock.new(Mutex.new, 1)
        @client_locks[client_id] = lock
        begin
          lock.mutex.synchronize { yield }
        ensure
          # Re-read: waiters that arrived meanwhile have raised the count.
          current = @client_locks[client_id]
          if current.users > 1
            @client_locks[client_id] = current.copy_with(users: current.users - 1)
          else
            @client_locks.delete(client_id)
          end
        end
      end

      private def remove_client(client) : Nil
        with_client_lock(client.client_id) { remove_client_locked(client) }
      end

      private def remove_client_locked(client) : Nil
        session = client.session
        if session.client.nil? || (session.client == client)
          session.client = nil
          session.delete if session.auto_delete?
        end
        client_id = client.client_id
        @clients.delete(client_id) if @clients[client_id]? == client
        @vhost.rm_connection(client)
      end

      def publish(packet : Protocol::Publish, publisher : String)
        @retain_store.retain(packet) if packet.retain?
        @exchange.publish(packet, publisher)
      end

      # Retain Handling [MQTT-3.3.1-9/10/11], spelled as the spec words it.
      #
      # Careful if you check this against the local MQTT-v5.0-spec.txt: its
      # Appendix B row for [MQTT-3.3.1-10] states the value-1 case inverted.
      # Four body locations agree against it - §3.3.1.3's definition of that
      # statement, §3.8.3.1's list of the three values, and §3.8.4's separate
      # new-vs-replaced rules - so the body governs.
      private def replay_retained?(retain_handling : Protocol::Subscribe::RetainHandling, new_subscription : Bool) : Bool
        case retain_handling
        in .send_on_subscribe?        then true
        in .send_on_new_subscription? then new_subscription
        in .do_not_send?              then false
        end
      end

      def subscribe(client, topics) : Array(Protocol::SubAck::ReasonCode)
        session = client.session
        topics.map do |tf|
          # We only deliver up to MAX_QOS, so grant (and store/deliver at) the
          # clamped QoS - the SUBACK must report the granted max [MQTT-3.8.4-7].
          options = SubscriptionOptions.new(
            MQTT.granted_qos(tf.qos), tf.no_local?, tf.retain_as_published?)
          new_subscription = session.subscribe(tf.topic, options)
          if replay_retained?(tf.retain_handling, new_subscription)
            @retain_store.each(tf.topic) do |retained|
              props = retained.properties
              # The lower of the publish and the subscription QoS [MQTT-3.8.4-8].
              props.delivery_mode = Math.min(props.delivery_mode || 0u8, options.qos)
              # The original timestamp, so the replay counts down the Message
              # Expiry Interval like any stored message [MQTT-3.3.2-6].
              msg = Message.new(retained.timestamp, EXCHANGE, retained.topic, props,
                retained.bodysize, retained.body_io)
              session.publish(msg)
            end
          end
          Protocol::SubAck::ReasonCode.from_value(options.qos)
        end
      end

      def unsubscribe(client, topics) : Array(Protocol::UnsubAck::ReasonCode)
        session = client.session
        topics.map do |tf|
          if session.unsubscribe(tf)
            Protocol::UnsubAck::ReasonCode::Success
          else
            Protocol::UnsubAck::ReasonCode::NoSubscriptionExisted
          end
        end
      end

      def close
        @retain_store.close
      end
    end
  end
end
