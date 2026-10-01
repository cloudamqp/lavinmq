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
      # Connection lifecycle rules, which the takeover [MQTT-3.1.4-2] relies on:
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
      # client's session present [MQTT-3.1.2-4]. Raises before anything is
      # sent, so the CONNECT can still be refused.
      private def add_client(io, connection_info, user, packet) : {Client, Bool}
        with_client_lock(packet.client_id) { add_client_locked(io, connection_info, user, packet) }
      end

      private def add_client_locked(io, connection_info, user, packet) : {Client, Bool}
        client_id = packet.client_id
        if prev_client = @clients[client_id]?
          prev_client.close(
            "New client #{connection_info.remote_address} " \
            "(username=#{packet.username}) connected as #{client_id}")
          remove_client_locked(prev_client)
        end
        existing = sessions[client_id]?
        # A clean session starts with no state at all [MQTT-3.1.2-6], and a
        # clean session's state lasts only as long as its connection.
        if existing && (packet.clean_session? || existing.clean_session?)
          existing.delete
          existing = nil
        end
        session = begin
          sessions.declare(client_id, packet.clean_session?)
        rescue Sessions::LimitReached
          raise Protocol::Error::ServerUnavailable.new(
            "queue limit (#{@vhost.max_queues}) reached in vhost \"#{@vhost.name}\"")
        rescue ex : Sessions::NameTaken
          # Retrying cannot help until an operator removes that queue.
          raise Protocol::Error::IdentifierRejected.new(
            "queue \"#{ex.message}\" in vhost \"#{@vhost.name}\" is not an MQTT session")
        end
        client = MQTT::Client.new(io,
          connection_info,
          user,
          self,
          session,
          client_id,
          ProtocolVersion.from_value(packet.version),
          packet.keepalive,
          packet.will)
        @clients[client_id] = client
        @vhost.add_connection client
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
          session.delete if session.clean_session?
        end
        client_id = client.client_id
        @clients.delete(client_id) if @clients[client_id]? == client
        @vhost.rm_connection(client)
      end

      def publish(packet : Protocol::Publish)
        @retain_store.retain(packet) if packet.retain?
        @exchange.publish(packet)
      end

      def subscribe(client, topics) : Array(Protocol::SubAck::ReturnCode)
        session = client.session
        headers = AMQP::Table.new({RETAIN_HEADER => true})
        topics.map do |tf|
          # `Subscribe.from_io` has already rejected anything above 2.
          qos = tf.qos
          session.subscribe(tf.topic, qos)
          ts = RoughTime.unix_ms
          @retain_store.each(tf.topic) do |topic, body_io, body_bytesize|
            props = AMQP::Properties.new(headers: headers, delivery_mode: qos)
            msg = Message.new(ts, EXCHANGE, topic, props, body_bytesize, body_io)
            session.publish(msg)
          end
          Protocol::SubAck::ReturnCode.from_int(qos)
        end
      end

      def unsubscribe(client, topics)
        session = client.session
        topics.each do |tf|
          session.unsubscribe(tf)
        end
      end

      def close
        @retain_store.close
      end
    end
  end
end
