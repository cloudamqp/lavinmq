require "../client"
require "../logger"
require "../rough_time"
require "../connection_info"
require "../stats"
require "./frame"
require "./frames"
require "./address"
require "./consumer"
require "./messaging"

module LavinMQ
  module AMQP10
    # An AMQP 1.0 connection. Maps the 1.0 model (sessions, links, transfers)
    # onto LavinMQ's core: a receiver link (client is sender) publishes to an
    # exchange/queue; a sender link (client is receiver) consumes from a queue.
    class Client < LavinMQ::Client
      include Stats
      include SortableJSON

      Log = LavinMQ::Log.for "amqp10.client"

      getter vhost : VHost
      getter user : Auth::BaseUser
      getter connection_info : ConnectionInfo
      getter name : String
      getter max_frame_size : UInt32
      getter container_id : String

      @connected_at = RoughTime.unix_ms
      @running = true
      @write_lock = Mutex.new(:checked)
      @sessions = Hash(UInt16, Session).new
      @dynamic_queues = [] of String
      # Reusable frame buffers: @input for the (single-fiber) read loop, @output
      # for outbound frame assembly under @write_lock. Avoids per-frame heap
      # allocation on the message hot path.
      @input = IO::Memory.new
      @output = IO::Memory.new
      rate_stats({"send_oct", "recv_oct"})

      # Default link credit granted to a publishing client.
      INCOMING_CREDIT = 256_u32

      def initialize(@socket : IO, @connection_info : ConnectionInfo, @vhost : VHost,
                     @user : Auth::BaseUser, @container_id : String,
                     @max_frame_size : UInt32, idle_timeout : UInt32?)
        @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
        metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s})
        @log = Logger.new(Log, metadata)
        if idle_timeout && idle_timeout > 0
          s = @socket
          s.read_timeout = (idle_timeout / 1000 / 2).seconds if s.responds_to?(:"read_timeout=")
        end
        @vhost.add_connection(self)
        @log.info { "Connection established for user=#{@user.name}" }
        spawn read_loop, name: "AMQP10::Client#read_loop #{@connection_info.remote_address}"
      end

      # Tracks an AMQP 1.0 session and its links (by handle).
      class Session
        getter channel : UInt16
        getter consumers = Hash(UInt32, Consumer).new
        getter receivers = Hash(UInt32, ReceiverLink).new

        def initialize(@channel : UInt16)
        end
      end

      # A receiver link on the broker: the client publishes to it.
      class ReceiverLink
        getter handle : UInt32
        getter name : String
        getter target : Address::Target?
        property credit : UInt32 = 0_u32
        property delivery_count : UInt32 = 0_u32
        # delivery-id -> accumulating payload, for multi-frame (more) transfers
        getter partials = Hash(UInt32, IO::Memory).new

        def initialize(@handle : UInt32, @name : String, @target : Address::Target?)
        end
      end

      private def read_loop
        loop do
          frame = Frame.read(@socket, @input)
          @recv_oct_count.add(8_u64, :relaxed)
          next if frame.heartbeat?
          perf = frame.performative
          next unless perf
          handle_performative(frame.channel, perf, frame.payload)
          break unless @running
        end
      rescue IO::TimeoutError
        FrameWriter.heartbeat(@socket) rescue nil
        retry_read_loop
      rescue ex : Error::Decode
        @log.warn { "decode error: #{ex.message}" }
      rescue ex : IO::Error
        @log.debug { "connection closed: #{ex.message}" } unless closed?
      rescue ex
        @log.error(exception: ex) { "unexpected error in read loop" }
      ensure
        cleanup
      end

      # Re-enter the read loop after sending a heartbeat on idle timeout.
      private def retry_read_loop
        read_loop if @running
      end

      private def handle_performative(channel : UInt16, perf : Described, payload : Bytes) : Nil
        case perf.descriptor.as?(UInt64)
        when Descriptor::BEGIN       then on_begin(channel, Begin.decode(perf))
        when Descriptor::ATTACH      then on_attach(channel, Attach.decode(perf))
        when Descriptor::FLOW        then on_flow(channel, Flow.decode(perf))
        when Descriptor::TRANSFER    then on_transfer(channel, Transfer.decode(perf), payload)
        when Descriptor::DISPOSITION then on_disposition(channel, Disposition.decode(perf))
        when Descriptor::DETACH      then on_detach(channel, Detach.decode(perf))
        when Descriptor::END_        then on_end(channel)
        when Descriptor::CLOSE       then on_close(Close.decode(perf))
        else                              @log.warn { "unhandled performative 0x#{perf.descriptor.as?(UInt64).try(&.to_s(16))}" }
        end
      end

      private def on_begin(channel : UInt16, frame : Begin) : Nil
        @sessions[channel] = Session.new(channel)
        reply = Begin.new(next_outgoing_id: 0_u32, incoming_window: 2048_u32,
          outgoing_window: 2048_u32, remote_channel: channel)
        write_frame(channel) { |b| reply.to_io(b) }
      end

      private def on_end(channel : UInt16) : Nil
        if session = @sessions.delete(channel)
          session.consumers.each_value(&.close)
        end
        write_frame(channel) { |b| End.new.to_io(b) }
      end

      private def on_close(frame : Close) : Nil
        write_frame(0_u16) { |b| Close.new.to_io(b) }
        @running = false
      end

      private def on_attach(channel : UInt16, frame : Attach) : Nil
        session = @sessions[channel]?
        return unless session
        if frame.role? == Attach::ROLE_SENDER
          attach_receiver_link(session, frame) # client sends -> broker receives -> publish
        else
          attach_sender_link(session, frame) # client receives -> broker sends -> consume
        end
      rescue ex : Error
        detach_with_error(channel, frame.handle, ErrorCondition::NOT_FOUND, ex.message || "")
      end

      # Client wants to publish: resolve target, grant credit.
      private def attach_receiver_link(session : Session, frame : Attach) : Nil
        tgt = frame.target
        if tgt && tgt.dynamic?
          # RabbitMQ 4.1 behaviour: implicitly create a server-named exclusive
          # auto-delete queue and echo its address back to the client.
          queue_name = declare_dynamic_queue
          target = Address::Target.new("", queue_name)
          reply_target = Target.new(address: queue_name, dynamic: true)
        else
          target = Address.parse_target(tgt.try(&.address))
          if target && !target.exchange.empty? && !@vhost.exchange_exists?(target.exchange)
            raise Error.new("Exchange '#{target.exchange}' not found")
          end
          if target && target.exchange.empty? && !target.routing_key.empty? && !@vhost.queue?(target.routing_key)
            raise Error.new("Queue '#{target.routing_key}' not found")
          end
          reply_target = tgt
        end
        link = ReceiverLink.new(frame.handle, frame.name, target)
        link.credit = INCOMING_CREDIT
        session.receivers[frame.handle] = link
        # Reply attach (mirror), then grant the client credit to send.
        reply = Attach.new(name: frame.name, handle: frame.handle, role: Attach::ROLE_RECEIVER,
          source: frame.source, target: reply_target)
        write_frame(session.channel) { |b| reply.to_io(b) }
        send_flow(session, handle: frame.handle, delivery_count: 0_u32, link_credit: INCOMING_CREDIT)
      end

      # Client wants to consume: resolve source queue, create consumer.
      private def attach_sender_link(session : Session, frame : Attach) : Nil
        src = frame.source
        if src && src.dynamic?
          # RabbitMQ 4.1 behaviour: dynamic source -> server-named exclusive
          # auto-delete queue (the classic reply-to / temporary-queue mechanism).
          queue_name = declare_dynamic_queue
          reply_source = Source.new(address: queue_name, dynamic: true)
        else
          queue_name = Address.parse_source(src.try(&.address))
          reply_source = src
        end
        q = @vhost.queue?(queue_name)
        raise Error.new("Queue '#{queue_name}' not found") unless q
        queue = q.as(LavinMQ::AMQP::Queue)
        unless @user.can_read?(@vhost.name, queue_name)
          raise Error.new("Access refused to queue '#{queue_name}'")
        end
        no_ack = frame.snd_settle_mode == Attach::SND_SETTLED
        idc = frame.initial_delivery_count || 0_u32
        consumer = Consumer.new(self, queue, frame.handle, frame.name, no_ack, idc)
        session.consumers[frame.handle] = consumer
        # Reply attach as sender, echoing our initial-delivery-count.
        reply = Attach.new(name: frame.name, handle: frame.handle, role: Attach::ROLE_SENDER,
          source: reply_source, target: frame.target, initial_delivery_count: idc)
        write_frame(session.channel) { |b| reply.to_io(b) }
        queue.add_consumer(consumer)
      end

      # Create a server-named exclusive, auto-delete queue for a dynamic node.
      private def declare_dynamic_queue : String
        name = LavinMQ::AMQP::Queue.generate_name
        @vhost.apply(AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name,
          false, false, true, true, false, AMQP::Table.new))
        @dynamic_queues << name
        name
      end

      private def on_flow(channel : UInt16, frame : Flow) : Nil
        session = @sessions[channel]?
        return unless session
        if handle = frame.handle
          if consumer = session.consumers[handle]?
            consumer.grant_credit(frame.link_credit || 0_u32, frame.delivery_count)
          end
        end
      end

      private def on_transfer(channel : UInt16, frame : Transfer, payload : Bytes) : Nil
        session = @sessions[channel]?
        return unless session
        link = session.receivers[frame.handle]?
        return unless link
        # Accumulate multi-frame deliveries.
        delivery_id = frame.delivery_id
        if frame.more?
          if id = delivery_id
            (link.partials[id] ||= IO::Memory.new).write(payload)
          end
          return
        end
        full = if (id = delivery_id) && (buf = link.partials.delete(id))
                 buf.write(payload)
                 buf.to_slice
               else
                 payload
               end
        publish(link, full)
        link.delivery_count &+= 1
        # Settle: tell the client we accepted it (unless they pre-settled).
        write_accept(channel, delivery_id) if delivery_id && !frame.settled
        replenish_credit(session, link)
      end

      private def publish(link : ReceiverLink, payload : Bytes) : Nil
        parsed = MessageCodec.parse(payload)
        target = link.target
        exchange = target.try(&.exchange) || ""
        routing_key = target.try(&.routing_key) || ""
        unless @user.can_write?(@vhost.name, exchange)
          @log.warn { "Access refused: user '#{@user.name}' may not publish to '#{exchange}'" }
          return
        end
        msg = LavinMQ::Message.new(RoughTime.unix_ms, exchange, routing_key,
          parsed.properties, parsed.body.size.to_u64, IO::Memory.new(parsed.body))
        @vhost.publish(msg)
      end

      private def replenish_credit(session : Session, link : ReceiverLink) : Nil
        return if link.credit > INCOMING_CREDIT // 2
        link.credit = INCOMING_CREDIT
        send_flow(session, handle: link.handle, delivery_count: link.delivery_count, link_credit: INCOMING_CREDIT)
      end

      private def on_disposition(channel : UInt16, frame : Disposition) : Nil
        session = @sessions[channel]?
        return unless session
        outcome = DeliveryState.classify(frame.state)
        first = frame.first
        last = frame.last || first
        (first..last).each do |delivery_id|
          session.consumers.each_value(&.settle(delivery_id, outcome))
        end
      end

      private def on_detach(channel : UInt16, frame : Detach) : Nil
        session = @sessions[channel]?
        return unless session
        session.consumers.delete(frame.handle).try(&.close)
        session.receivers.delete(frame.handle)
        write_frame(channel) { |b| Detach.new(handle: frame.handle, closed: true).to_io(b) }
      end

      private def send_flow(session : Session, handle : UInt32, delivery_count : UInt32, link_credit : UInt32) : Nil
        flow = Flow.new(incoming_window: 2048_u32, next_outgoing_id: 0_u32, outgoing_window: 2048_u32,
          next_incoming_id: 0_u32, handle: handle, delivery_count: delivery_count, link_credit: link_credit)
        write_frame(session.channel) { |b| flow.to_io(b) }
      end

      # Send a transfer + message (called by Consumer on delivery). Builds the
      # transfer performative and message sections straight into the reusable
      # output buffer — no per-message allocations.
      def deliver_message(handle : UInt32, delivery_id : UInt32, settled : Bool, msg : LavinMQ::BytesMessage) : Nil
        channel = consumer_channel(handle)
        write_frame(channel) do |b|
          Codec.write_described_list(b, Descriptor::TRANSFER, 6) do |l|
            Codec.write_uint(l, handle)      # handle
            Codec.write_uint(l, delivery_id) # delivery-id
            l.write_byte Codec::VBIN8        # delivery-tag: 4-byte delivery-id
            l.write_byte 4_u8
            delivery_id.to_io(l, Codec::BE)
            Codec.write_uint(l, 0_u32)   # message-format
            Codec.write_bool(l, settled) # settled
            Codec.write_bool(l, false)   # more
          end
          MessageCodec.encode(b, msg.properties, msg.body)
        end
      end

      # Send a settled "accepted" disposition for an inbound (published) transfer.
      private def write_accept(channel : UInt16, delivery_id : UInt32) : Nil
        write_frame(channel) do |b|
          Codec.write_described_list(b, Descriptor::DISPOSITION, 5) do |l|
            Codec.write_bool(l, Attach::ROLE_RECEIVER) # role
            Codec.write_uint(l, delivery_id)           # first
            Codec.write_uint(l, delivery_id)           # last
            Codec.write_bool(l, true)                  # settled
            # state: accepted = described(0x24, list0)
            l.write_byte Codec::DESCRIBED
            Codec.write_ulong(l, Descriptor::ACCEPTED)
            l.write_byte Codec::LIST0
          end
        end
      end

      private def consumer_channel(handle : UInt32) : UInt16
        @sessions.each do |channel, session|
          return channel if session.consumers.has_key?(handle)
        end
        0_u16
      end

      private def write_frame(channel : UInt16, & : IO::Memory ->) : Nil
        return if closed?
        @write_lock.synchronize do
          FrameWriter.write(@socket, @output, channel) { |b| yield b }
          @socket.flush
        end
        @send_oct_count.add(8_u64, :relaxed)
      rescue ex : IO::Error
        @log.debug { "lost connection while sending: #{ex.message}" } unless closed?
        close_socket
      end

      def flush : Nil
        @write_lock.synchronize { @socket.flush } unless closed?
      rescue IO::Error
      end

      private def detach_with_error(channel : UInt16, handle : UInt32, condition : String, description : String) : Nil
        detach = Detach.new(handle: handle, closed: true,
          error: ErrorPerformative.new(condition, description))
        write_frame(channel) { |b| detach.to_io(b) }
      end

      private def cleanup : Nil
        @running = false
        @sessions.each_value { |s| s.consumers.each_value(&.close) }
        @sessions.clear
        # Remove implicitly-created dynamic queues when the connection ends.
        @dynamic_queues.each { |name| @vhost.delete_queue(name) rescue nil }
        @dynamic_queues.clear
        @vhost.rm_connection(self)
        close_socket
        @log.info { "Connection disconnected for user=#{@user.name}" }
      end

      private def close_socket : Nil
        @running = false
        @socket.close
      rescue ex
        @log.debug { "#{ex.inspect} when closing socket" }
      end

      def close(reason = "") : Nil
        return if closed?
        @log.info { "Closing connection: #{reason}" }
        write_frame(0_u16) { |b| Close.new(error: ErrorPerformative.new(ErrorCondition::CONNECTION_FORCED, reason)).to_io(b) }
        @running = false
        close_socket
      end

      def force_close : Nil
        close_socket
      end

      def closed? : Bool
        !@running
      end

      # ---- management / polymorphic Client interface ----

      def client_name : String
        @container_id
      end

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

      def state : String
        closed? ? "closed" : (@vhost.flow? ? "running" : "flow")
      end

      def details_tuple
        {
          channels:          0,
          connected_at:      @connected_at,
          type:              "network",
          channel_max:       0_u16,
          frame_max:         @max_frame_size,
          timeout:           0_u16,
          client_properties: NamedTuple.new,
          vhost:             @vhost.name,
          user:              @user.name,
          protocol:          "AMQP 1.0",
          auth_mechanism:    "PLAIN",
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
        }.merge(current_stats_details)
      end

      def to_json(json : JSON::Builder)
        details_tuple.merge(stats_details).to_json(json)
      end

      def search_match?(value : String) : Bool
        @name.includes?(value) || @user.name.includes?(value)
      end

      def search_match?(value : Regex) : Bool
        value === @name || value === @user.name
      end
    end
  end
end
