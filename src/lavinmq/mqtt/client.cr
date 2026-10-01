require "openssl"
require "socket"
require "../client"
require "../error"
require "../rough_time"
require "./session"
require "./protocol"
require "../bool_channel"
require "./consts"
require "../stats"
require "../persister"
require "sync/exclusive"

module LavinMQ
  module MQTT
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON
      include Persister::ConfirmTarget

      # An acknowledgement packet (3.1.1 4.3) that leaves once the state it
      # answers for is durable. `seq` orders them, so the persister's
      # cumulative confirm releases every one up to it. A barrier carries the
      # routing generation its PUBLISH_RECEIVED record is written for.
      record PendingAck, seq : UInt64, type : PacketType, packet_id : UInt16, generation : UInt32? = nil do
        enum PacketType : UInt8
          PubAck
          PubRec
          PubRel
          PubComp
        end

        def barrier? : Bool
          !generation.nil?
        end
      end

      getter log, name, user, client_id, socket, connection_info, session
      @connected_at = RoughTime.unix_ms
      @started = false
      getter? closed = false
      @channels = Hash(UInt16, Client::Channel).new
      @ack_seq = 0u64
      @pending_acks = Sync::Exclusive(Deque(PendingAck)).new(Deque(PendingAck).new, :unchecked)
      # Created with the ack writer fiber on the first queued ack of any kind (PUBACK, PUBREC, PUBREL, PUBCOMP)
      @ack_mailbox : ::Channel(UInt64)?
      # Set by the read loop on exit; the ack writer outlives it while barriers are queued
      @read_loop_done = false
      rate_stats({"send_oct", "recv_oct"})
      Log = LavinMQ::Log.for "mqtt.client"

      def vhost
        @broker.vhost
      end

      # Stub channel accessors for polymorphic dispatch with AMQP::Client

      def channel_count : Int32
        0
      end

      def each_channel(& : LavinMQ::Client::Channel ->) : Nil
      end

      def channels : Array(LavinMQ::Client::Channel)
        [] of LavinMQ::Client::Channel
      end

      def channel?(id : UInt16) : LavinMQ::Client::Channel?
        nil
      end

      def initialize(@io : Protocol::IO,
                     @connection_info : ConnectionInfo,
                     @user : Auth::BaseUser,
                     @broker : MQTT::Broker,
                     @session : MQTT::Session,
                     @client_id : String,
                     @keepalive : UInt16 = 30,
                     @will : Protocol::Will? = nil)
        @permission_context = PermissionService::Context.new(@user.name, @client_id)
        @lock = Mutex.new
        @waitgroup = WaitGroup.new(1)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @connection_info.remote_address.to_s, client_id: client_id})
        @log = Logger.new(Log, metadata)
      end

      # Attaching can yield on the store lock, so it comes after `@started`,
      # which makes a takeover's `close` wait for this fiber to finish.
      def run : Nil
        @started = true
        @session.client = self
        @log.info { "Connection established for user=#{@user.name}" }
        case user = @user
        when Auth::OAuthUser
          user.on_expiration do
            close("token expired")
          end
        end
        read_loop
      ensure
        @waitgroup.done
      end

      def client_name
        "mqtt-client-#{@client_id}"
      end

      private def protocol_name : String
        case @io.version
        when .v5?   then "MQTT 5.0"
        when .v3_1? then "MQTT 3.1"
        else             "MQTT 3.1.1"
        end
      end

      private def read_loop
        received_bytes = 0_u32
        socket = @io.io
        if socket.responds_to?(:"read_timeout=")
          # 50% grace period according to [MQTT-3.1.2-24]
          socket.read_timeout = @keepalive.zero? ? nil : (@keepalive * 1.5).seconds
        end
        loop do
          @log.trace { "waiting for packet" }
          packet, bytesize = read_and_handle_packet
          if (received_bytes &+= bytesize) > Config.instance.yield_each_received_bytes
            received_bytes = 0_u32
            Fiber.yield
          end
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          if packet.is_a?(Protocol::Disconnect)
            @log.debug { "Received disconnect" }
            break
          end
        end
      rescue ex : Session::ProtocolViolation | Session::AwaitingPubrelLimitReached
        # The Will publishes from here as it does on every other close without a
        # DISCONNECT [MQTT-3.1.2-8]; 3.1.2.5 names a server close on a protocol
        # error as one of those situations.
        @log.warn { "Closing connection: #{ex.message}" }
        publish_will
      rescue ex : Protocol::Error::PacketDecode
        @log.warn(exception: ex) { "Packet decode error" }
        publish_will
      rescue ex : ::IO::TimeoutError
        @log.warn { "Keepalive timeout (keepalive:#{@keepalive}): #{ex.message}" }
        publish_will
      rescue ex : ::IO::Error
        @log.error { "Client unexpectedly closed connection: #{ex.message}" } unless closed_by_server?
        publish_will
      rescue ex
        @log.error(exception: ex) { "Read Loop error" }
        publish_will
      ensure
        case user = @user
        when Auth::OAuthUser
          user.cleanup
        end
        stop_ack_writer
        close_socket
        @log.info { "Connection disconnected for user=#{@user.name} duration=#{duration}" }
      end

      # A deleted session closes only the socket, not the client, and logs why.
      private def closed_by_server? : Bool
        @closed || @session.deleted?
      end

      private def duration
        ms = RoughTime.unix_ms - @connected_at
        seconds = (ms / 1000).round.to_i
        Time::Span.new(seconds: seconds)
      end

      def read_and_handle_packet
        packet = @io.read_packet
        @log.trace { "Received packet:  #{packet.inspect}" }
        bytesize = @io.bytesize(packet)
        @recv_oct_count.add(bytesize, :relaxed)
        vhost.add_recv_bytes(bytesize.to_u64)

        case packet
        when Protocol::Publish     then recieve_publish(packet)
        when Protocol::PubAck      then recieve_puback(packet)
        when Protocol::PubRec      then recieve_pubrec(packet)
        when Protocol::PubRel      then recieve_pubrel(packet)
        when Protocol::PubComp     then recieve_pubcomp(packet)
        when Protocol::Subscribe   then recieve_subscribe(packet)
        when Protocol::Unsubscribe then recieve_unsubscribe(packet)
        when Protocol::PingReq     then receive_pingreq(packet)
        when Protocol::Disconnect  then return {packet, bytesize}
        else                            raise "received unexpected packet: #{packet}"
        end
        {packet, bytesize}
      end

      def send(packet)
        @lock.synchronize do
          @io.write_packet(packet)
          @io.flush
          bytesize = @io.bytesize(packet)
          @send_oct_count.add(bytesize, :relaxed)
          vhost.add_send_bytes(bytesize.to_u64)
        end
        case packet
        when Protocol::Publish
          if packet.dup?
            vhost.event_tick(EventType::ClientRedeliver)
          else
            vhost.event_tick(EventType::ClientDeliverNoAck) if packet.qos == 0
            vhost.event_tick(EventType::ClientDeliver) if packet.qos > 0
          end
        when Protocol::PubAck, Protocol::PubRec
          vhost.event_tick(EventType::ClientPublishConfirm)
        end
      end

      def receive_pingreq(packet : Protocol::PingReq)
        send Protocol::PingResp.new
      end

      def recieve_publish(packet : Protocol::Publish)
        validate_packet_id(packet)
        if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
          Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
          close_socket
          return
        end
        packet_id = packet.packet_id
        # A topic denial acks and drops, it never closes the connection. QoS 2
        # takes a PUBREC, and the PUBREL that follows is answered by
        # `recieve_pubrel` like any unknown id.
        unless @broker.permission_service.can_write?(@permission_context, packet.topic)
          Log.debug { "Publish refused: no topic permission rule allows user '#{@user.name}' (client '#{@client_id}') to write topic '#{packet.topic}'" }
          # Queued like the others, so acknowledgements leave in publish order
          if packet.qos > 0 && packet_id
            queue_ack(packet.qos == 2u8 ? PendingAck::PacketType::PubRec : PendingAck::PacketType::PubAck, packet_id)
          end
          return
        end
        if packet.qos == 2 && packet_id
          recieve_qos2_publish(packet, packet_id)
          return
        end
        @broker.publish(packet)
        vhost.event_tick(EventType::ClientPublish)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && packet_id
          queue_ack(packet.qos == 2u8 ? PendingAck::PacketType::PubRec : PendingAck::PacketType::PubAck, packet_id)
        end
      end

      # Id 0 is illegal [MQTT-2.3.1-1]; held as a QoS 2 id it would also
      # dedupe every later PUBLISH that carried it.
      private def validate_packet_id(packet : Protocol::Publish) : Nil
        if packet.qos > 0 && packet.packet_id == 0
          raise Session::ProtocolViolation.new("QoS #{packet.qos} PUBLISH with packet id 0")
        end
      end

      # The packet is sent by the ack writer, so neither the read loop nor the
      # session's fibers wait for the disk.
      def queue_ack(type : PendingAck::PacketType, packet_id : UInt16, generation : UInt32? = nil) : Nil
        unless @ack_mailbox
          mailbox = @ack_mailbox = ::Channel(UInt64).new(1)
          spawn ack_writer(mailbox), name: "MQTT client #{@client_id} ack writer"
        end
        seq = @ack_seq &+= 1
        @pending_acks.lock &.push(PendingAck.new(seq, type, packet_id, generation))
        vhost.enqueue_ack(self, seq)
      end

      # Non-blocking; if the 1-slot mailbox is full, the stale seq is dropped
      # (confirms are cumulative).
      def enqueue_confirm_ack(msgid : UInt64) : Nil
        mailbox = @ack_mailbox || return
        loop do
          return if mailbox.try_send(msgid)
          mailbox.try_receive?
        end
      rescue ::Channel::ClosedError
      end

      private def ack_writer(mailbox : ::Channel(UInt64))
        while seq = mailbox.receive?
          while pending = next_ack(seq)
            if pending.barrier?
              fire_barriers(pending, seq)
              break
            end
            send_ack(pending)
          end
          mailbox.close if @read_loop_done && !barriers_queued?
        end
      rescue ex : PacketIdLog::Error
        @log.error(exception: ex) { "Failed to record a QoS 2 packet id" }
        close("packet id log write failed")
      end

      # A dead socket only drops the packet: a barrier behind it still has a
      # record to write, or a re-sent PUBLISH would be routed again. Closed on
      # the first failure, so nothing follows a packet that may be half written.
      # Any error, so the writer outlives whatever the transport raises.
      private def send_ack(pending : PendingAck) : Nil
        send(ack_packet(pending))
      rescue ::IO::Error | OpenSSL::SSL::Error
        close_socket
      rescue ex
        @log.warn(exception: ex) { "Failed to send #{pending.type}" }
        close_socket
      end

      # The writer holds this connection's socket, so after a takeover it never
      # sends on the new one, and its records go to the shared session.
      private def stop_ack_writer : Nil
        @read_loop_done = true
        @ack_mailbox.try &.close unless barriers_queued?
      end

      private def barriers_queued? : Bool
        @pending_acks.lock &.any?(&.barrier?)
      end

      # Two drains before a QoS 2 PUBREC: the routing, then the id record, so
      # power loss cannot keep an id without its message. Every entry the
      # confirm covers goes back under one new seq, so a burst costs two drains.
      private def fire_barriers(first : PendingAck, seq : UInt64) : Nil
        covered = [first]
        while pending = next_ack(seq)
          covered << pending
        end
        # A closed session skips the record and the PUBREC still goes out:
        # harmless, the client stops re-sending and its PUBREL gets PUBCOMP.
        covered.each do |p|
          if generation = p.generation
            @session.record_publish_received(p.packet_id, generation)
          end
        end
        requeue_acks(covered)
      end

      # Back at the head, in order, so everything behind them waits too
      private def requeue_acks(covered : Array(PendingAck)) : Nil
        seq = @ack_seq &+= 1
        @pending_acks.lock do |queue|
          covered.reverse_each { |p| queue.unshift(p.copy_with(seq: seq, generation: nil)) }
        end
        vhost.enqueue_ack(self, seq)
      end

      private def ack_packet(pending : PendingAck) : Protocol::Packet
        id = pending.packet_id
        case pending.type
        in .pub_ack?  then Protocol::PubAck.new(id)
        in .pub_rec?  then Protocol::PubRec.new(id)
        in .pub_rel?  then Protocol::PubRel.new(id)
        in .pub_comp? then Protocol::PubComp.new(id)
        end
      end

      private def next_ack(seq : UInt64) : PendingAck?
        @pending_acks.lock do |pending|
          pending.shift if pending.first?.try(&.seq.<= seq)
        end
      end

      # Figure 4.3: store the id, route, then answer PUBREC once durable.
      # Dedupe is by id alone: a recipient cannot assume a `dup` PUBLISH is one
      # it has seen (3.3.1.1).
      private def recieve_qos2_publish(packet : Protocol::Publish, packet_id : UInt16)
        if @session.publish_received(packet_id)
          begin
            @broker.publish(packet)
          rescue ex
            # An id left behind by a routing failure would dedupe away the
            # client's re-send, turning a duplicate into silent loss.
            @session.pubrel_received(packet_id)
            raise ex
          end
          vhost.event_tick(EventType::ClientPublish)
          queue_ack(PendingAck::PacketType::PubRec, packet_id, @session.publish_routed(packet_id))
          return
        end
        # A re-send means our first PUBREC was lost. On the same connection it
        # queues behind the original's barrier; after a takeover it can beat
        # the old writer's record, but its drain still covers the routing.
        queue_ack(PendingAck::PacketType::PubRec, packet_id)
      end

      def recieve_pubrec(packet : Protocol::PubRec)
        vhost.event_tick(EventType::ClientAck) if @session.pubrec(packet)
      end

      def recieve_pubcomp(packet : Protocol::PubComp)
        @session.pubcomp(packet)
      end

      def recieve_pubrel(packet : Protocol::PubRel)
        id = packet.packet_id
        unless @session.pubrel_received(id)
          # PUBCOMP is the only answer that lets the client release the id, and
          # an unknown id is ordinary: a new clean session holds none of the
          # client's ids, and a topic denial PUBRECs without holding the id.
          @log.debug { "PUBREL for unknown packet id '#{id}', answering PUBCOMP anyway" }
        end
        queue_ack(PendingAck::PacketType::PubComp, id)
      end

      def recieve_puback(packet : Protocol::PubAck)
        @session.puback(packet)
        vhost.event_tick(EventType::ClientAck)
      end

      def recieve_subscribe(packet : Protocol::Subscribe)
        if Config.instance.mqtt_permission_check_enabled?
          unless user.can_read?(@broker.vhost.name, EXCHANGE) && user.can_write?(@broker.vhost.name, "mqtt.#{client_id}")
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            close_socket
            return
          end
        end
        # Topic permissions are enforced at delivery, not at SUBSCRIBE, so a client
        # may subscribe to a filter it cannot read. Mosquitto also filters at
        # delivery, but it additionally refuses the filter in the SUBACK.
        qos = @broker.subscribe(self, packet.topic_filters)
        send(Protocol::SubAck.new(qos, packet.packet_id))
      end

      def recieve_unsubscribe(packet : Protocol::Unsubscribe)
        @broker.unsubscribe(self, packet.topics)
        send(Protocol::UnsubAck.new(packet.packet_id))
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              @user.name,
          protocol:          protocol_name,
          client_id:         @client_id,
          name:              @name,
          timeout:           @keepalive,
          connected_at:      @connected_at,
          state:             state,
          host:              @connection_info.local_address.address,
          port:              @connection_info.local_address.port,
          peer_host:         @connection_info.remote_address.address,
          peer_port:         @connection_info.remote_address.port,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          client_properties: NamedTuple.new,
        }.merge(current_stats_details)
      end

      def to_json(json : JSON::Builder)
        details_tuple.merge(stats_details).to_json(json)
      end

      def search_match?(value : String) : Bool
        @name.includes?(value) ||
          @user.name.includes?(value)
      end

      def search_match?(value : Regex) : Bool
        value === @name ||
          value === @user.name
      end

      private def publish_will
        if will = @will
          if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            return
          end
          unless @broker.permission_service.can_write?(@permission_context, will.topic)
            Log.debug { "Will publish refused: no topic permission rule allows user '#{@user.name}' (client '#{@client_id}') to write topic '#{will.topic}'" }
            return
          end
          @broker.publish(Protocol::Publish.new(
            topic: will.topic,
            payload: will.payload,
            packet_id: nil,
            qos: will.qos,
            retain: will.retain?,
            dup: false,
          ))
        end
      rescue ex
        @log.warn { "Failed to publish will: #{ex.message}" }
      end

      # should only be used when server needs to froce close client
      #
      # A client that never started has no read fiber to wait for:
      # `Broker#run_client` sees `closed?` and does not start it.
      def close(reason = "")
        return if @closed
        @log.info { "Closing connection: #{reason}" }
        @closed = true
        close_socket
        @waitgroup.wait if @started
      end

      def state
        @closed ? "closed" : (@broker.vhost.flow? ? "running" : "flow")
      end

      def force_close
        close_socket
      end

      private def close_socket
        socket = @io.io
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = 1.seconds
        end
        socket.close
      rescue ::IO::Error
      end
    end
  end
end
