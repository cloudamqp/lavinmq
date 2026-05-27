require "openssl"
require "socket"
require "../client"
require "../error"
require "../rough_time"
require "./session"
require "./protocol"
require "../bool_channel"
require "./consts"
require "./sparkplug/validator"
require "./sparkplug/certificate_mapper"
require "./sparkplug/protobuf_validator"

module LavinMQ
  module MQTT
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter log, name, user, client_id, socket, connection_info
      getter? clean_session
      @connected_at = RoughTime.unix_ms
      @channels = Hash(UInt16, Client::Channel).new
      @session : MQTT::Session?
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

      def initialize(@io : MQTT::IO,
                     @connection_info : ConnectionInfo,
                     @user : Auth::BaseUser,
                     @broker : MQTT::Broker,
                     @client_id : String,
                     @clean_session : Bool = false,
                     @keepalive : UInt16 = 30,
                     @will : MQTT::Will? = nil)
        @lock = Mutex.new
        @waitgroup = WaitGroup.new(1)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @broker.vhost.name, address: @connection_info.remote_address.to_s, client_id: client_id})
        @log = Logger.new(Log, metadata)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop, name: "MQTT read_loop #{@connection_info.remote_address}"
        case user = @user
        when Auth::OAuthUser
          user.on_expiration do
            close("token expired")
          end
        end
      end

      def client_name
        "mqtt-client-#{@client_id}"
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
          packet = read_and_handle_packet
          if (received_bytes &+= packet.bytesize) > Config.instance.yield_each_received_bytes
            received_bytes = 0_u32
            Fiber.yield
          end
          # The disconnect packet has been handled and the socket has been closed.
          # If we dont breakt the loop here we'll get a IO/Error on next read.
          if packet.is_a?(MQTT::Disconnect)
            @log.debug { "Received disconnect" }
            break
          end
        end
      rescue ex : ::MQTT::Protocol::Error::PacketDecode
        @log.warn(exception: ex) { "Packet decode error" }
        publish_will
      rescue ex : ::IO::TimeoutError
        @log.warn { "Keepalive timeout (keepalive:#{@keepalive}): #{ex.message}" }
        publish_will
      rescue ex : ::IO::Error
        @log.error { "Client unexpectedly closed connection: #{ex.message}" } unless @closed
        publish_will
      rescue ex
        @log.error(exception: ex) { "Read Loop error" }
        publish_will
      ensure
        @broker.remove_client(self)
        case user = @user
        when Auth::OAuthUser
          user.cleanup
        end
        @waitgroup.done
        close_socket
        @log.info { "Connection disconnected for user=#{@user.name} duration=#{duration}" }
      end

      private def duration
        ms = RoughTime.unix_ms - @connected_at
        seconds = (ms / 1000).round.to_i
        Time::Span.new(seconds: seconds)
      end

      def read_and_handle_packet
        packet = @io.read_packet
        @log.trace { "Received packet:  #{packet.inspect}" }
        @recv_oct_count.add(packet.bytesize, :relaxed)
        vhost.add_recv_bytes(packet.bytesize.to_u64)

        case packet
        when MQTT::Publish     then recieve_publish(packet)
        when MQTT::PubAck      then recieve_puback(packet)
        when MQTT::Subscribe   then recieve_subscribe(packet)
        when MQTT::Unsubscribe then recieve_unsubscribe(packet)
        when MQTT::PingReq     then receive_pingreq(packet)
        when MQTT::Disconnect  then return packet
        else                        raise "received unexpected packet: #{packet}"
        end
        packet
      end

      def send(packet)
        @lock.synchronize do
          @io.write_packet(packet)
          @io.flush
          @send_oct_count.add(packet.bytesize, :relaxed)
          vhost.add_send_bytes(packet.bytesize.to_u64)
        end
        case packet
        when MQTT::Publish
          if packet.dup?
            vhost.event_tick(EventType::ClientRedeliver)
          else
            vhost.event_tick(EventType::ClientDeliverNoAck) if packet.qos == 0
            vhost.event_tick(EventType::ClientDeliver) if packet.qos > 0
          end
        when MQTT::PubAck
          vhost.event_tick(EventType::ClientPublishConfirm)
        end
      end

      def receive_pingreq(packet : MQTT::PingReq)
        send MQTT::PingResp.new
      end

      def recieve_publish(packet : MQTT::Publish)
        if Config.instance.mqtt_permission_check_enabled? && !user.can_write?(@broker.vhost.name, EXCHANGE)
          Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
          close_socket
          return
        end

        # Sparkplug validation if enabled
        if @broker.vhost.sparkplug_aware?
          begin
            packet = sparkplug_validate(packet)
          rescue ex : Sparkplug::ValidationError
            Log.warn { "Sparkplug validation error: #{ex.message}" }
            close_socket
            return
          end
        end

        @broker.publish(packet)
        vhost.event_tick(EventType::ClientPublish)
        # Ok to not send anything if qos = 0 (fire and forget)
        if packet.qos > 0 && (packet_id = packet.packet_id)
          send(MQTT::PubAck.new(packet_id))
        end
      end

      # Validates a publish against the Sparkplug 3.0 spec, returning the packet
      # to publish (BIRTH messages are forced to be retained). Raises
      # `Sparkplug::ValidationError` for anything that violates the spec.
      private def sparkplug_validate(packet : MQTT::Publish) : MQTT::Publish
        if packet.topic.starts_with?("spBv3.0/")
          parts = Sparkplug::Validator.parse_topic(packet.topic)
          unless parts
            raise Sparkplug::ValidationError.new("Invalid Sparkplug topic: #{packet.topic}")
          end
          msg_type = Sparkplug::Validator.validate_topic(parts)

          # Validate protobuf payload
          result = Sparkplug::ProtobufValidator.validate_payload(packet.payload, msg_type)
          unless result.valid?
            raise Sparkplug::ValidationError.new("Payload validation failed: #{result.error}")
          end

          # Auto-retain BIRTH messages
          if (msg_type.nbirth? || msg_type.dbirth?) && !packet.retain?
            packet = MQTT::Publish.new(
              topic: packet.topic,
              payload: packet.payload,
              packet_id: packet.packet_id,
              qos: packet.qos,
              retain: true,
              dup: packet.dup?
            )
          end
        elsif Sparkplug::Validator.certificate_topic?(packet.topic)
          # Reject publishing to certificate topics
          raise Sparkplug::ValidationError.new("Cannot publish to $sparkplug/certificates topics")
        end
        packet
      end

      def recieve_puback(packet : MQTT::PubAck)
        @broker.sessions[@client_id].ack(packet)
        vhost.event_tick(EventType::ClientAck)
      end

      def recieve_subscribe(packet : MQTT::Subscribe)
        if Config.instance.mqtt_permission_check_enabled?
          unless user.can_read?(@broker.vhost.name, EXCHANGE) && user.can_write?(@broker.vhost.name, "mqtt.#{client_id}")
            Log.debug { "Access refused: user '#{user.name}' does not have permissions" }
            close_socket
            return
          end
        end

        unless @broker.vhost.sparkplug_aware?
          qos = @broker.subscribe(self, packet.topic_filters)
          send(MQTT::SubAck.new(qos, packet.packet_id))
          return
        end

        send(MQTT::SubAck.new(sparkplug_subscribe(packet.topic_filters), packet.packet_id))
      end

      # Subscribes, expanding $sparkplug/certificates filters into the actual
      # BIRTH topics. Returns exactly one SubAck return code per *original*
      # filter (MQTT 3.1.1 §3.9): a certificate filter that expands to no real
      # topic is reported as a failure.
      private def sparkplug_subscribe(topic_filters) : Array(MQTT::SubAck::ReturnCode)
        return_codes = Array(MQTT::SubAck::ReturnCode).new(topic_filters.size)
        expanded = Array(MQTT::Subscribe::TopicFilter).new(topic_filters.size)
        topic_filters.each do |tf|
          if Sparkplug::CertificateMapper.certificate_topic?(tf.topic)
            before = expanded.size
            Sparkplug::CertificateMapper.expand_certificate_subscription(tf.topic) do |actual_topic|
              expanded << MQTT::Subscribe::TopicFilter.new(actual_topic, tf.qos)
            end
            if expanded.size == before
              # Certificate filter that maps to no real topic
              return_codes << MQTT::SubAck::ReturnCode::Failure
            else
              return_codes << MQTT::SubAck::ReturnCode.from_int(tf.qos)
            end
          else
            expanded << tf
            return_codes << MQTT::SubAck::ReturnCode.from_int(tf.qos)
          end
        end
        @broker.subscribe(self, expanded)
        return_codes
      end

      def recieve_unsubscribe(packet : MQTT::Unsubscribe)
        @broker.unsubscribe(client_id, packet.topics)
        send(MQTT::UnsubAck.new(packet.packet_id))
      end

      def details_tuple
        {
          vhost:             @broker.vhost.name,
          user:              @user.name,
          protocol:          "MQTT 3.1.1",
          client_id:         @client_id,
          name:              @name,
          timeout:           @keepalive,
          connected_at:      @connected_at,
          state:             state,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          client_properties: NamedTuple.new,
        }.merge(stats_details)
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
          @broker.publish(MQTT::Publish.new(
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
      def close(reason = "")
        return if @closed
        @log.info { "Closing connection: #{reason}" }
        @closed = true
        close_socket
        @waitgroup.wait
      end

      def state
        @closed ? "closed" : (@broker.vhost.flow? ? "running" : "flow")
      end

      def force_close
        close_socket
      end

      private def close_socket
        socket = @io
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = 1.seconds
        end
        socket.close
      rescue ::IO::Error
      end
    end

    class Consumer < LavinMQ::Client::Channel::Consumer
      # `MQTT::Consumer` only has the `has_capacity` method to satisfy the interface.
      # Since it's never used a shared object can be used. It's also closed immediately
      # to not have a fiber running.
      class_getter(dummy_has_capacity : BoolChannel) { BoolChannel.new(true).tap &.close }

      getter unacked = 0_u32
      getter tag : String

      def has_capacity : BoolChannel
        self.class.dummy_has_capacity
      end

      property prefetch_count = 0_u16

      def initialize(@client : Client, @session : MQTT::Session)
        @tag = "mqtt.#{@client.client_id}"
      end

      def details_tuple
        {
          queue: {
            name:  "mqtt.#{@client.client_id}",
            vhost: @client.vhost.name,
          },
          consumer_tag:    @tag,
          exclusive:       exclusive?,
          ack_required:    !no_ack?,
          prefetch_count:  @prefetch_count,
          priority:        priority,
          channel_details: {
            peer_host:       @client.connection_info.remote_address.address,
            peer_port:       @client.connection_info.remote_address.port,
            connection_name: @client.name,
            user:            @client.user.name,
            number:          0_u16,
            name:            "#{@client.connection_info.remote_address}[0]",
          },
        }
      end

      def no_ack?
        true
      end

      def accepts? : Bool
        true
      end

      def deliver(msg : MQTT::Publish)
        @client.send(msg)
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        raise NotImplementedError.new("MQTT Consumer can't deliver AMQP messages")
      end

      def exclusive?
        true
      end

      def cancel
        @client.close("Server force closed client")
      end

      def close
        @client.close("Server force closed client")
      end

      def closed?
        false
      end

      def flow(active : Bool)
        raise NotImplementedError.new("MQTT Consumer doesn't support flow")
      end

      def ack(sp)
        raise NotImplementedError.new("MQTT Consumer doesn't support ack")
      end

      def reject(sp, requeue = false)
        raise NotImplementedError.new("MQTT Consumer doesn't support reject")
      end

      def priority
        0
      end
    end
  end
end
