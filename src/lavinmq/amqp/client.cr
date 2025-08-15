require "openssl"
require "socket"
require "./channel"
require "../client"
require "../error"
require "../logger"
require "../name_validator"
require "./channel_reply_code"
require "./connection_reply_code"
require "../rough_time"
require "../connection_info"

module LavinMQ
  module AMQP
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      getter vhost, log, name
      getter user
      getter max_frame_size : UInt32
      getter channel_max : UInt16
      getter heartbeat_timeout : UInt16
      getter auth_mechanism : String
      getter client_properties : AMQP::Table
      getter connection_info : ConnectionInfo

      @connected_at = RoughTime.unix_ms
      @channels = Sync::Shared(Hash(UInt16, Client::Channel)).new(Hash(UInt16, Client::Channel).new)
      @actual_channel_max : UInt16
      @exclusive_queues = Array(Queue).new
      @heartbeat_interval_ms : Int64?
      @running = true
      @last_recv_frame = RoughTime.monotonic
      @last_sent_frame = RoughTime.monotonic
      rate_stats({"send_oct", "recv_oct"})
      DEFAULT_EX = "amq.default"
      Log        = LavinMQ::Log.for "amqp.client"

      def initialize(@socket : IO,
                     @connection_info : ConnectionInfo,
                     @vhost : VHost,
                     @user : User,
                     tune_ok,
                     start_ok)
        @max_frame_size = tune_ok.frame_max

        # keep 0 = unlimited in ui/api for consistency with the spec
        @channel_max = tune_ok.channel_max
        # use @actual_channel_max for limit check
        @actual_channel_max = @channel_max.zero? ? UInt16::MAX : @channel_max

        @heartbeat_timeout = tune_ok.heartbeat
        @heartbeat_interval_ms = tune_ok.heartbeat.zero? ? nil : ((tune_ok.heartbeat / 2) * 1000).to_i64
        @auth_mechanism = start_ok.mechanism
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        @client_properties = start_ok.client_properties
        connection_name = @client_properties["connection_name"]?.try(&.as?(String))
        @metadata =
          if connection_name
            ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s, name: connection_name})
          else
            ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s})
          end
        @log = Logger.new(Log, @metadata)
        @vhost.add_connection(self)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop, name: "Client#read_loop #{@connection_info.remote_address}"
      end

      # Returns client provided connection name if set, else server generated name
      def client_name
        @client_properties["connection_name"]?.try(&.as(String)) || @name
      end

      def channel_name_prefix
        @connection_info.remote_address.to_s
      end

      def details_tuple
        {
          channels:          @channels.read &.size,
          connected_at:      @connected_at,
          type:              "network",
          channel_max:       @channel_max,
          frame_max:         @max_frame_size,
          timeout:           @heartbeat_timeout,
          client_properties: @client_properties,
          vhost:             @vhost.name,
          user:              @user.name,
          protocol:          "AMQP 0-9-1",
          auth_mechanism:    @auth_mechanism,
          host:              @connection_info.local_address.address,
          port:              @connection_info.local_address.port,
          peer_host:         @connection_info.remote_address.address,
          peer_port:         @connection_info.remote_address.port,
          name:              @name,
          pid:               @name,
          ssl:               @connection_info.ssl?,
          tls_version:       @connection_info.ssl_version,
          cipher:            @connection_info.ssl_cipher,
          state:             state,
        }.merge(stats_details)
      end

      def search_match?(value : String) : Bool
        @name.includes?(value) ||
          @user.name.includes?(value) ||
          @client_properties["connection_name"]?.try(&.to_s.includes?(value)) || false
      end

      def search_match?(value : Regex) : Bool
        value === @name ||
          value === @user.name ||
          value === @client_properties["connection_name"]?
      end

      private def read_loop
        received_bytes = 0_u32
        socket = @socket
        loop do
          AMQP::Frame.from_io(socket) do |frame|
            {% unless flag?(:release) %}
              @log.trace { "Received #{frame.inspect}" }
            {% end %}
            if (received_bytes &+= frame.bytesize) > Config.instance.yield_each_received_bytes
              received_bytes = 0_u32
              Fiber.yield
            end
            frame_size_ok?(frame) || return
            case frame
            when AMQP::Frame::Connection::Close
              @log.debug { "Client disconnected: #{frame.reply_text}" } unless frame.reply_text.empty?
              send AMQP::Frame::Connection::CloseOk.new
              @running = false
              next
            when AMQP::Frame::Connection::CloseOk
              @log.debug { "Confirmed disconnect" }
              @running = false
              return
            end
            if @running
              process_frame(frame)
            else
              case frame
              when AMQP::Frame::Body
                @log.debug { "Skipping body, waiting for CloseOk" }
                frame.body.skip(frame.body_size)
              else
                @log.debug { "Discarding #{frame.class.name}, waiting for CloseOk" }
              end
            end
          rescue e : LavinMQ::Error::PreconditionFailed
            send_precondition_failed(frame, e.message)
          end
        rescue IO::TimeoutError
          send_heartbeat || break
        rescue ex : AMQ::Protocol::Error::NotImplemented
          @log.error { ex.inspect }
          send_not_implemented(ex)
        rescue ex : AMQ::Protocol::Error::FrameDecode
          @log.error(exception: ex) { "AMQP frame decode error" }
          send_frame_error(ex.message)
          break
        rescue ex : IO::Error | OpenSSL::SSL::Error
          @log.debug(exception: ex) { "Lost connection, while reading (#{ex.inspect})" } unless closed?
          break
        rescue ex : Exception
          @log.error(exception: ex) { "Unexpected error, while reading: #{ex.message}" }
          send_internal_error(ex.message)
          break
        end
      ensure
        cleanup
        close_socket
        @log.info { "Connection disconnected for user=#{@user.name}" }
      end

      private def frame_size_ok?(frame) : Bool
        if frame.bytesize > @max_frame_size
          send_frame_error("frame size #{frame.bytesize} exceeded max #{@max_frame_size} bytes")
          return false
        end
        true
      end

      private def send_heartbeat
        now = RoughTime.monotonic
        if @last_recv_frame + (@heartbeat_timeout + 5).seconds < now
          @log.info { "Heartbeat timeout (#{@heartbeat_timeout}), last seen frame #{(now - @last_recv_frame).total_seconds} s ago, sent frame #{(now - @last_sent_frame).total_seconds} s ago" }
          false
        else
          send AMQP::Frame::Heartbeat.new
        end
      end

      def send(frame : AMQP::Frame, channel_is_open : Bool? = nil) : Bool
        return false if closed?
        if channel_is_open.nil?
          channel_is_open = frame.channel.zero? || fetch_channel(frame.channel).try &.running?
        end
        unless channel_is_open
          @log.debug { "Channel #{frame.channel} is closed so is not sending #{frame.inspect}" }
          return false
        end
        {% unless flag?(:release) %}
          @log.trace { "Send #{frame.inspect}" }
        {% end %}
        @write_lock.synchronize do
          s = @socket
          s.write_bytes frame, IO::ByteFormat::NetworkEndian
          s.flush
        end
        @last_sent_frame = RoughTime.monotonic
        @send_oct_count.add(8_u64 + frame.bytesize, :relaxed)
        if frame.is_a?(AMQP::Frame::Connection::CloseOk)
          return false
        end
        true
      rescue ex : IO::Error | OpenSSL::SSL::Error
        @log.debug { "Lost connection, while sending (#{ex.inspect})" } unless closed?
        close_socket
        false
      rescue ex : IO::TimeoutError
        @log.info { "Timeout while sending (#{ex.inspect})" }
        close_socket
        false
      rescue ex
        @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
        send_internal_error(ex.message)
        false
      end

      def connection_details
        {
          peer_host: @connection_info.remote_address.address,
          peer_port: @connection_info.remote_address.port,
          name:      @name,
        }
      end

      @write_lock = Mutex.new(:checked)

      def deliver(frame, msg, flush = true)
        return false if closed?
        @write_lock.synchronize do
          socket = @socket
          websocket = socket.is_a? WebSocketIO
          {% unless flag?(:release) %}
            @log.trace { "Send #{frame.inspect}" }
          {% end %}
          socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
          socket.flush if websocket
          @send_oct_count.add(8_u64 + frame.bytesize, :relaxed)
          header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.bodysize, msg.properties)
          {% unless flag?(:release) %}
            @log.trace { "Send #{header.inspect}" }
          {% end %}
          socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
          socket.flush if websocket
          @send_oct_count.add(8_u64 + header.bytesize, :relaxed)
          pos = 0
          while pos < msg.bodysize
            length = Math.min(msg.bodysize - pos, @max_frame_size - 8).to_u32
            {% unless flag?(:release) %}
              @log.trace { "Send BodyFrame (pos #{pos}, length #{length})" }
            {% end %}
            body = case msg
                   in BytesMessage
                     AMQP::Frame::BytesBody.new(frame.channel, length, msg.body[pos, length])
                   in Message
                     AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
                   end
            socket.write_bytes body, ::IO::ByteFormat::NetworkEndian
            socket.flush if websocket
            @send_oct_count.add(8_u64 + body.bytesize, :relaxed)
            pos += length
          end
          socket.flush if flush && !websocket # Websockets need to send one frame per WS frame
          @last_sent_frame = RoughTime.monotonic
        end
        true
      rescue ex : IO::Error | OpenSSL::SSL::Error
        @log.debug { "Lost connection, while sending message (#{ex.inspect})" } unless closed?
        close_socket
        Fiber.yield
        false
      rescue ex : AMQ::Protocol::Error::FrameEncode
        @log.warn { "Error encoding frame (#{ex.inspect})" }
        close_socket
        false
      rescue ex : IO::TimeoutError
        @log.info { "Timeout while sending (#{ex.inspect})" }
        close_socket
        false
      rescue ex
        @log.error { "Delivery exception: #{ex.inspect_with_backtrace}" }
        raise ex
      end

      def state
        !@running ? "closed" : (@vhost.flow? ? "running" : "flow")
      end

      private def with_channel(frame, &)
        if ch = fetch_channel(frame.channel)
          if ch.running?
            yield ch
          else
            case frame
            when AMQP::Frame::Basic::Publish, AMQP::Frame::Header
              @log.trace { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            when AMQP::Frame::Body
              @log.trace { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
              frame.body.skip(frame.body_size)
            else
              @log.trace { "Discarding #{frame.inspect}, waiting for Close(Ok)" }
            end
          end
        else
          case frame
          when AMQP::Frame::Basic::Publish, AMQP::Frame::Header
            @log.trace { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
          when AMQP::Frame::Body
            @log.trace { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            frame.body.skip(frame.body_size)
          else
            @log.error { "Channel #{frame.channel} not open while processing #{frame.class.name}" }
            close_connection(frame, ConnectionReplyCode::CHANNEL_ERROR, "Channel #{frame.channel} not open")
          end
        end
      end

      private def open_channel(frame)
        if @channels.read &.has_key? frame.channel
          close_connection(frame, ConnectionReplyCode::CHANNEL_ERROR, "second 'channel.open' seen")
        elsif (size = channels_count) >= @actual_channel_max
          reply_text = "number of channels opened (#{size})" \
                       " has reached the negotiated channel_max (#{@actual_channel_max})"
          close_connection(frame, ConnectionReplyCode::NOT_ALLOWED, reply_text)
        else
          @channels.write &.[frame.channel] = AMQP::Channel.new(self, frame.channel)
          @vhost.event_tick(EventType::ChannelCreated)
          send AMQP::Frame::Channel::OpenOk.new(frame.channel)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def process_frame(frame) : Nil
        @last_recv_frame = RoughTime.monotonic
        @recv_oct_count.add(8_u64 + frame.bytesize, :relaxed)
        case frame
        when AMQP::Frame::Channel::Open
          open_channel(frame)
        when AMQP::Frame::Channel::Close
          @channels.write &.delete(frame.channel).try &.close
          send AMQP::Frame::Channel::CloseOk.new(frame.channel), true
        when AMQP::Frame::Channel::CloseOk
          @channels.write &.delete(frame.channel).try &.close
        when AMQP::Frame::Channel::Flow
          with_channel frame, &.flow(frame.active)
        when AMQP::Frame::Channel::FlowOk
          # noop
        when AMQP::Frame::Confirm::Select
          with_channel frame, &.confirm_select(frame)
        when AMQP::Frame::Exchange::Declare
          declare_exchange(frame)
        when AMQP::Frame::Exchange::Delete
          delete_exchange(frame)
        when AMQP::Frame::Exchange::Bind
          bind_exchange(frame)
        when AMQP::Frame::Exchange::Unbind
          unbind_exchange(frame)
        when AMQP::Frame::Queue::Declare
          declare_queue(frame)
        when AMQP::Frame::Queue::Bind
          bind_queue(frame)
        when AMQP::Frame::Queue::Unbind
          unbind_queue(frame)
        when AMQP::Frame::Queue::Delete
          delete_queue(frame)
        when AMQP::Frame::Queue::Purge
          purge_queue(frame)
        when AMQP::Frame::Basic::Publish
          start_publish(frame)
        when AMQP::Frame::Header
          with_channel frame, &.next_msg_headers(frame)
        when AMQP::Frame::Body
          with_channel frame, &.add_content(frame)
        when AMQP::Frame::Basic::Consume
          consume(frame)
        when AMQP::Frame::Basic::Get
          basic_get(frame)
        when AMQP::Frame::Basic::Ack
          with_channel frame, &.basic_ack(frame)
        when AMQP::Frame::Basic::Reject
          with_channel frame, &.basic_reject(frame)
        when AMQP::Frame::Basic::Nack
          with_channel frame, &.basic_nack(frame)
        when AMQP::Frame::Basic::Cancel
          with_channel frame, &.cancel_consumer(frame)
        when AMQP::Frame::Basic::Qos
          with_channel frame, &.basic_qos(frame)
        when AMQP::Frame::Basic::Recover
          with_channel frame, &.basic_recover(frame)
        when AMQP::Frame::Tx::Select
          with_channel frame, &.tx_select(frame)
        when AMQP::Frame::Tx::Commit
          with_channel frame, &.tx_commit(frame)
        when AMQP::Frame::Tx::Rollback
          with_channel frame, &.tx_rollback(frame)
        when AMQP::Frame::Heartbeat
          nil
        else
          send_not_implemented(frame)
        end
        if heartbeat_interval_ms = @heartbeat_interval_ms
          if @last_sent_frame + heartbeat_interval_ms.milliseconds < RoughTime.monotonic
            send AMQP::Frame::Heartbeat.new
          end
        end
      rescue ex : LavinMQ::Error::UnexpectedFrame
        @log.error { ex.inspect }
        close_channel(ex.frame, ChannelReplyCode::UNEXPECTED_FRAME, ex.frame.class.name)
      end

      private def cleanup
        @running = false
        i = 0u32
        @channels.write do |channels|
          channels.each_value do |ch|
            ch.close
            Fiber.yield if (i &+= 1) % 512 == 0
          end
          channels.clear
        end
        @exclusive_queues.each(&.close)
        @exclusive_queues.clear
        @vhost.rm_connection(self)
      end

      private def close_socket
        @running = false
        @socket.close
      rescue ex
        @log.debug { "#{ex.inspect} when closing socket" }
      end

      def close(reason = nil, timeout : Time::Span = 5.seconds)
        reason ||= "Connection closed"
        @log.info { "Closing: #{reason}" }

        socket = @socket
        if socket.responds_to?(:"write_timeout=")
          socket.write_timeout = timeout
          socket.read_timeout = timeout
        end

        code = ConnectionReplyCode::CONNECTION_FORCED
        send AMQP::Frame::Connection::Close.new(code.value, "#{code} - #{reason}", 0_u16, 0_u16)
        @running = false
      end

      def force_close
        close_socket
      end

      def closed?
        !@running
      end

      def close_channel(frame : AMQ::Protocol::Frame, code : ChannelReplyCode, text)
        if frame.channel.zero?
          return close_connection(frame, ConnectionReplyCode::UNEXPECTED_FRAME, text)
        end
        text = "#{code} - #{text}"
        case frame
        when AMQ::Protocol::Frame::Method
          send AMQP::Frame::Channel::Close.new(frame.channel, code.value, text, frame.class_id, frame.method_id)
        else
          send AMQP::Frame::Channel::Close.new(frame.channel, code.value, text, 0, 0)
        end
        @channels.write &.delete(frame.channel).try &.close
      end

      def close_connection(frame : AMQ::Protocol::Frame?, code : ConnectionReplyCode, text)
        text = "#{code} - #{text}"
        @log.info { "Closing, #{text}" }
        case frame
        when AMQ::Protocol::Frame::Method
          send AMQP::Frame::Connection::Close.new(code.value, text, frame.class_id, frame.method_id)
        else
          send AMQP::Frame::Connection::Close.new(code.value, text, 0_u16, 0_u16)
        end
        @log.info { "Connection=#{@name} disconnected" }
      ensure
        @running = false
      end

      def send_access_refused(frame, text)
        @log.warn { "Access refused channel=#{frame.channel} reason=\"#{text}\"" }
        close_channel(frame, ChannelReplyCode::ACCESS_REFUSED, text)
      end

      def send_not_found(frame, text = "")
        @log.warn { "Not found channel=#{frame.channel} reason=\"#{text}\"" }
        close_channel(frame, ChannelReplyCode::NOT_FOUND, text)
      end

      def send_resource_locked(frame, text)
        @log.warn { "Resource locked channel=#{frame.channel} reason=\"#{text}\"" }
        close_channel(frame, ChannelReplyCode::RESOURCE_LOCKED, text)
      end

      def send_precondition_failed(frame, text)
        @log.warn { "Precondition failed channel=#{frame.channel} reason=\"#{text}\"" }
        close_channel(frame, ChannelReplyCode::PRECONDITION_FAILED, text)
      end

      def send_not_implemented(frame, text = nil)
        @log.error { "#{frame.inspect}, not implemented reason=\"#{text}\"" }
        close_channel(frame, ChannelReplyCode::NOT_IMPLEMENTED, text)
      end

      def send_not_implemented(ex : AMQ::Protocol::Error::NotImplemented)
        code = ConnectionReplyCode::NOT_IMPLEMENTED
        if ex.channel.zero?
          send AMQP::Frame::Connection::Close.new(code.value, code.to_s, ex.class_id, ex.method_id)
          @running = false
        else
          send AMQP::Frame::Channel::Close.new(ex.channel, code.value, code.to_s, ex.class_id, ex.method_id)
          @channels.write &.delete(ex.channel).try &.close
        end
      end

      def send_internal_error(message)
        close_connection(nil, ConnectionReplyCode::INTERNAL_ERROR, "Unexpected error, please report")
      end

      def send_frame_error(message = nil)
        close_connection(nil, ConnectionReplyCode::FRAME_ERROR, message)
      end

      private def declare_exchange(frame)
        if !NameValidator.valid_entity_name?(frame.exchange_name)
          send_precondition_failed(frame, "Exchange name isn't valid")
        elsif frame.exchange_name.empty?
          send_access_refused(frame, "Not allowed to declare the default exchange")
        elsif e = @vhost.fetch_exchange(frame.exchange_name)
          redeclare_exchange(e, frame)
        elsif frame.passive
          send_not_found(frame, "Exchange '#{frame.exchange_name}' doesn't exists")
        elsif NameValidator.reserved_prefix?(frame.exchange_name)
          send_access_refused(frame, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
        else
          ae = frame.arguments["x-alternate-exchange"]?.try &.as?(String)
          ae_ok = ae.nil? || (@user.can_write?(@vhost.name, ae) && @user.can_read?(@vhost.name, frame.exchange_name))
          unless ae_ok && @user.can_config?(@vhost.name, frame.exchange_name)
            send_access_refused(frame, "User doesn't have permissions to declare exchange '#{frame.exchange_name}'")
            return
          end
          begin
            @vhost.apply(frame)
          rescue e : LavinMQ::Error::ExchangeTypeError
            send_precondition_failed(frame, e.message)
          end
          send AMQP::Frame::Exchange::DeclareOk.new(frame.channel) unless frame.no_wait
        end
      end

      private def redeclare_exchange(e, frame)
        if frame.passive || e.match?(frame)
          unless frame.no_wait
            send AMQP::Frame::Exchange::DeclareOk.new(frame.channel)
          end
        else
          send_precondition_failed(frame, "Existing exchange '#{frame.exchange_name}' declared with other arguments")
        end
      end

      private def delete_exchange(frame)
        if !NameValidator.valid_entity_name?(frame.exchange_name)
          send_precondition_failed(frame, "Exchange name isn't valid")
        elsif frame.exchange_name.empty?
          send_access_refused(frame, "Not allowed to delete the default exchange")
        elsif NameValidator.reserved_prefix?(frame.exchange_name)
          send_access_refused(frame, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
        elsif !@vhost.has_exchange?(frame.exchange_name)
          # should return not_found according to spec but we make it idempotent
          send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
        elsif !@user.can_config?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have permissions to delete exchange '#{frame.exchange_name}'")
        elsif frame.if_unused && @vhost.fetch_exchange(frame.exchange_name).try(&.in_use?)
          send_precondition_failed(frame, "Exchange '#{frame.exchange_name}' in use")
        else
          @vhost.apply(frame)
          send AMQP::Frame::Exchange::DeleteOk.new(frame.channel) unless frame.no_wait
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def delete_queue(frame)
        if frame.queue_name.empty? && @last_queue_name
          frame.queue_name = @last_queue_name.not_nil!
        end
        if !NameValidator.valid_entity_name?(frame.queue_name)
          send_precondition_failed(frame, "Queue name isn't valid")
          return
        end
        q = @vhost.fetch_queue(frame.queue_name)
        if q.nil?
          send AMQP::Frame::Queue::DeleteOk.new(frame.channel, 0_u32) unless frame.no_wait
        elsif queue_exclusive_to_other_client?(q)
          send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
        elsif frame.if_unused && !q.consumer_count.zero?
          send_precondition_failed(frame, "Queue '#{q.name}' in use")
        elsif frame.if_empty && !q.message_count.zero?
          send_precondition_failed(frame, "Queue '#{q.name}' is not empty")
        elsif !@user.can_config?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have permissions to delete queue '#{q.name}'")
        else
          size = q.message_count
          @vhost.apply(frame)
          @exclusive_queues.delete(q) if q.exclusive?
          send AMQP::Frame::Queue::DeleteOk.new(frame.channel, size) unless frame.no_wait
        end
      end

      def queue_exclusive_to_other_client?(q)
        q.exclusive? && !@exclusive_queues.includes?(q)
      end

      private def declare_queue(frame)
        if !frame.queue_name.empty? && !NameValidator.valid_entity_name?(frame.queue_name)
          send_precondition_failed(frame, "Queue name isn't valid")
        elsif q = @vhost.fetch_queue(frame.queue_name)
          redeclare_queue(frame, q)
        elsif {"amq.rabbitmq.reply-to", "amq.direct.reply-to"}.includes? frame.queue_name
          unless frame.no_wait
            send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
          end
        elsif frame.queue_name.starts_with?("amq.direct.reply-to.")
          consumer_tag = frame.queue_name[20..]
          if @vhost.direct_reply_consumers.has_key? consumer_tag
            send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 1_u32)
          else
            send_not_found(frame, "Queue '#{frame.queue_name}' doesn't exists")
          end
        elsif frame.passive
          send_not_found(frame, "Queue '#{frame.queue_name}' doesn't exists")
        elsif NameValidator.reserved_prefix?(frame.queue_name)
          send_access_refused(frame, "Prefix #{NameValidator::PREFIX_LIST} forbidden, please choose another name")
        elsif @vhost.max_queues.try { |max| @vhost.queues_count >= max }
          send_access_refused(frame, "queue limit in vhost '#{@vhost.name}' (#{@vhost.max_queues}) is reached")
        else
          declare_new_queue(frame)
        end
      end

      private def redeclare_queue(frame, q)
        if queue_exclusive_to_other_client?(q) || invalid_exclusive_redclare?(frame, q)
          send_resource_locked(frame, "Exclusive queue")
        elsif frame.passive || q.match?(frame)
          q.redeclare
          unless frame.no_wait
            send AMQP::Frame::Queue::DeclareOk.new(frame.channel, q.name,
              q.message_count, q.consumer_count)
          end
          @last_queue_name = frame.queue_name
        elsif frame.exclusive && !q.exclusive?
          send_resource_locked(frame, "Not an exclusive queue")
        else
          send_precondition_failed(frame, "Existing queue '#{q.name}' declared with other arguments")
        end
      end

      private def invalid_exclusive_redclare?(frame, q)
        q.exclusive? && !frame.passive && !frame.exclusive
      end

      @last_queue_name : String?

      private def declare_new_queue(frame)
        unless @vhost.flow?
          send_precondition_failed(frame, "Server low on disk space, can not create queue")
        end
        if frame.queue_name.empty?
          frame.queue_name = AMQP::Queue.generate_name
        end
        dlx = frame.arguments["x-dead-letter-exchange"]?.try &.as?(String)
        dlx_ok = dlx.nil? || (@user.can_write?(@vhost.name, dlx) && @user.can_read?(@vhost.name, name))
        unless dlx_ok && @user.can_config?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue_name}'")
          return
        end
        @vhost.apply(frame)
        @last_queue_name = frame.queue_name
        if frame.exclusive
          @exclusive_queues << @vhost.fetch_queue(frame.queue_name).not_nil!
        end
        unless frame.no_wait
          send AMQP::Frame::Queue::DeclareOk.new(frame.channel, frame.queue_name, 0_u32, 0_u32)
        end
      end

      private def bind_queue(frame)
        if frame.queue_name.empty? && @last_queue_name
          frame.queue_name = @last_queue_name.not_nil!
          # according to spec if both queue name and routing key is empty,
          # then substitute them with the name of the last declared queue
          if frame.routing_key.empty?
            frame.routing_key = @last_queue_name.not_nil!
          end
        end
        return unless valid_q_bind_unbind?(frame)

        q = @vhost.fetch_queue(frame.queue_name)
        if q.nil?
          send_not_found frame, "Queue '#{frame.queue_name}' not found"
        elsif !@vhost.has_exchange?(frame.exchange_name)
          send_not_found frame, "Exchange '#{frame.exchange_name}' not found"
        elsif !@user.can_read?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
        elsif !@user.can_write?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        elsif queue_exclusive_to_other_client?(q)
          send_resource_locked(frame, "Exclusive queue")
        else
          begin
            @vhost.apply(frame)
            send AMQP::Frame::Queue::BindOk.new(frame.channel) unless frame.no_wait
          rescue ex : LavinMQ::Exchange::AccessRefused
            send_access_refused(frame, ex.message)
          end
        end
      end

      private def unbind_queue(frame)
        if frame.queue_name.empty? && @last_queue_name
          frame.queue_name = @last_queue_name.not_nil!
        end
        return unless valid_q_bind_unbind?(frame)

        q = @vhost.fetch_queue(frame.queue_name)
        if q.nil?
          # should return not_found according to spec but we make it idempotent
          send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
        elsif !@vhost.has_exchange?(frame.exchange_name)
          # should return not_found according to spec but we make it idempotent
          send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
        elsif !@user.can_read?(@vhost.name, frame.exchange_name)
          send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.exchange_name}'")
        elsif !@user.can_write?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
        elsif queue_exclusive_to_other_client?(q)
          send_resource_locked(frame, "Exclusive queue")
        else
          begin
            @vhost.apply(frame)
            send AMQP::Frame::Queue::UnbindOk.new(frame.channel)
          rescue ex : LavinMQ::Exchange::AccessRefused
            send_access_refused(frame, ex.message)
          end
        end
      end

      private def valid_q_bind_unbind?(frame) : Bool
        if !NameValidator.valid_entity_name?(frame.queue_name)
          send_precondition_failed(frame, "Queue name isn't valid")
          return false
        elsif !NameValidator.valid_entity_name?(frame.exchange_name)
          send_precondition_failed(frame, "Exchange name isn't valid")
          return false
        end
        true
      end

      private def bind_exchange(frame)
        source = @vhost.fetch_exchange(frame.source)
        destination = @vhost.fetch_exchange(frame.destination)
        if destination.nil?
          send_not_found frame, "Exchange '#{frame.destination}' doesn't exists"
        elsif source.nil?
          send_not_found frame, "Exchange '#{frame.source}' doesn't exists"
        elsif !@user.can_read?(@vhost.name, frame.source)
          send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
        elsif !@user.can_write?(@vhost.name, frame.destination)
          send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
        elsif frame.source.empty? || frame.destination.empty?
          send_access_refused(frame, "Not allowed to bind to the default exchange")
        else
          @vhost.apply(frame)
          send AMQP::Frame::Exchange::BindOk.new(frame.channel) unless frame.no_wait
        end
      end

      private def unbind_exchange(frame)
        source = @vhost.fetch_exchange(frame.source)
        destination = @vhost.fetch_exchange(frame.destination)
        if destination.nil?
          # should return not_found according to spec but we make it idempotent
          send AMQP::Frame::Exchange::UnbindOk.new(frame.channel)
        elsif source.nil?
          # should return not_found according to spec but we make it idempotent
          send AMQP::Frame::Exchange::UnbindOk.new(frame.channel)
        elsif !@user.can_read?(@vhost.name, frame.source)
          send_access_refused(frame, "User doesn't have read permissions to exchange '#{frame.source}'")
        elsif !@user.can_write?(@vhost.name, frame.destination)
          send_access_refused(frame, "User doesn't have write permissions to exchange '#{frame.destination}'")
        elsif frame.source.empty? || frame.destination.empty? || frame.source == DEFAULT_EX || frame.destination == DEFAULT_EX
          send_access_refused(frame, "Not allowed to unbind from the default exchange")
        else
          @vhost.apply(frame)
          send AMQP::Frame::Exchange::UnbindOk.new(frame.channel) unless frame.no_wait
        end
      end

      private def purge_queue(frame)
        if frame.queue_name.empty? && @last_queue_name
          frame.queue_name = @last_queue_name.not_nil!
        end
        unless @user.can_read?(@vhost.name, frame.queue_name)
          send_access_refused(frame, "User doesn't have write permissions to queue '#{frame.queue_name}'")
          return
        end
        if !NameValidator.valid_entity_name?(frame.queue_name)
          send_precondition_failed(frame, "Queue name isn't valid")
        elsif q = @vhost.fetch_queue(frame.queue_name)
          if queue_exclusive_to_other_client?(q)
            send_resource_locked(frame, "Queue '#{q.name}' is exclusive")
          else
            messages_purged = q.purge
            send AMQP::Frame::Queue::PurgeOk.new(frame.channel, messages_purged) unless frame.no_wait
          end
        else
          send_not_found(frame, "Queue '#{frame.queue_name}' not found")
        end
      end

      @acl_cache = Hash({String, String}, Bool).new

      private def start_publish(frame)
        cache_key = {@vhost.name, frame.exchange}
        allowed = @acl_cache[cache_key]?
        if allowed.nil?
          allowed = @acl_cache[cache_key] = @user.can_write?(*cache_key)
        end
        if allowed
          with_channel frame, &.start_publish(frame)
        else
          send_access_refused(frame, "User not allowed to publish to exchange '#{frame.exchange}'")
        end
      end

      private def consume(frame)
        if frame.queue.empty? && @last_queue_name
          frame.queue = @last_queue_name.not_nil!
        end
        if !NameValidator.valid_entity_name?(frame.queue)
          send_precondition_failed(frame, "Queue name isn't valid")
          return
        end
        unless @user.can_read?(@vhost.name, frame.queue)
          send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
          return
        end
        with_channel frame, &.consume(frame)
      end

      private def basic_get(frame)
        if frame.queue.empty? && @last_queue_name
          frame.queue = @last_queue_name.not_nil!
        end
        if !NameValidator.valid_entity_name?(frame.queue)
          send_precondition_failed(frame, "Queue name isn't valid")
          return
        end
        unless @user.can_read?(@vhost.name, frame.queue)
          send_access_refused(frame, "User doesn't have permissions to queue '#{frame.queue}'")
          return
        end
        # yield so that msg expiration, consumer delivery etc gets priority
        Fiber.yield
        with_channel frame, &.basic_get(frame)
      end

      def flush
        @write_lock.synchronize do
          @socket.flush
        end
      end

      def check_consumer_timeout
        @channels.read &.each_value do |ch|
          ch.check_consumer_timeout
        end
      end

      def each_channel(&)
        @channels.read &.each_value do |ch|
          yield ch
        end
      end

      def channels_count
        @channels.read &.size
      end

      def channels
        @channels.read &.values
      end

      def fetch_channel(channel_id : UInt16)
        @channels.read &.[channel_id]?
      end
    end
  end
end
