require "../stats"
require "./client"

module LavinMQ::AMQP10
  abstract class Link
    getter session, name, remote_handle, local_handle, role, dynamic_queue
    getter delivery_count = 0_u32
    property? closed = false

    def initialize(@session : Session, @name : String, @remote_handle : UInt32,
                   @local_handle : UInt32, @role : Role, @dynamic_queue : LavinMQ::AMQP::Queue? = nil)
    end

    def close : Nil
      return if @closed
      @closed = true
      @session.client.close_dynamic_queue(@dynamic_queue)
    end
  end

  class ReceiverLink < Link
    @body_io = SliceIO.new
    @message_reader = SliceReader.new
    @partial_payload = IO::Memory.new
    @partial_delivery_id : UInt32?
    @partial_settled = false
    @partial_active = false
    @target : PublishAddress?

    def initialize(session : Session, name : String, remote_handle : UInt32,
                   local_handle : UInt32, @target : PublishAddress?, dynamic_queue : LavinMQ::AMQP::Queue? = nil)
      super(session, name, remote_handle, local_handle, Role::Receiver, dynamic_queue)
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def receive(transfer : TransferCodec::TransferView, payload : Bytes) : Nil
      return if closed?
      if transfer.aborted
        clear_partial
        return
      end
      if transfer.more
        append_fragment(transfer, payload)
        return
      end

      incoming_payload, delivery_id, settled = if @partial_active
                                                 append_fragment(transfer, payload)
                                                 {@partial_payload.to_slice, @partial_delivery_id || transfer.delivery_id, @partial_settled || transfer.settled}
                                               else
                                                 {payload, transfer.delivery_id, transfer.settled}
                                               end

      incoming = MessageCodec.decode(@message_reader.reset(incoming_payload))
      target = @target || begin
        to = incoming.to || raise ProtocolError.new("anonymous target requires properties.to")
        @session.client.resolve_publish_target(to)
      end
      validate_user_id(incoming.properties.user_id)
      if incoming.body.bytesize > Config.instance.max_message_size
        raise ProtocolError.new("message size #{incoming.body.bytesize} larger than max size #{Config.instance.max_message_size}")
      end

      @body_io.reset(incoming.body)
      msg = Message.new(RoughTime.unix_ms, target.exchange, target.routing_key,
        incoming.properties, incoming.body.bytesize.to_u64, @body_io)
      result = @session.publish(msg)
      outcome = if result.overflowed?
                  Outcome::Rejected
                elsif result.routed?
                  Outcome::Accepted
                else
                  Outcome::Released
                end
      settle(delivery_id, settled, outcome)
    rescue ex : ProtocolError | DecodeError | LavinMQ::Error::PreconditionFailed
      @session.client.@log.warn { "AMQP 1.0 publish rejected: #{ex.message}" }
      settle(@partial_delivery_id || transfer.delivery_id, @partial_settled || transfer.settled, Outcome::Rejected)
      clear_partial
    ensure
      clear_partial unless transfer.more
    end

    private def validate_user_id(user_id)
      current_user = @session.client.user
      if user_id && !user_id.empty? && user_id != current_user.name && !current_user.can_impersonate?
        raise ProtocolError.new("Message's user-id property '#{user_id}' does not match actual user '#{current_user.name}'")
      end
    end

    private def append_fragment(transfer, payload) : Nil
      if @partial_active
        if delivery_id = transfer.delivery_id
          stored = @partial_delivery_id
          raise ProtocolError.new("fragmented transfer delivery-id changed") if stored && stored != delivery_id
          @partial_delivery_id ||= delivery_id
        end
        @partial_settled ||= transfer.settled
      else
        @partial_payload.clear
        @partial_delivery_id = transfer.delivery_id
        @partial_settled = transfer.settled
        @partial_active = true
      end
      @partial_payload.write(payload)
      if @partial_payload.size > Config.instance.max_message_size
        raise ProtocolError.new("message size larger than max size #{Config.instance.max_message_size}")
      end
    end

    private def clear_partial : Nil
      return unless @partial_active
      @partial_payload.clear
      @partial_delivery_id = nil
      @partial_settled = false
      @partial_active = false
    end

    private def settle(delivery_id, settled, outcome)
      return if settled
      if delivery_id
        @session.client.send_disposition(@session, delivery_id, outcome)
      end
    end
  end

  class SenderLink < Link
    record Unack, delivery_id : UInt32, queue : LavinMQ::AMQP::Queue, sp : SegmentPosition, delivered_at : Time::Instant

    getter queue, credit = 0_u32
    @consumer : Consumer?
    @unacked = Deque(Unack).new
    @deliver_loop_running = Atomic(Bool).new(false)
    @credit_available = BoolChannel.new(false)
    @closed_channel = ::Channel(Nil).new
    @delivery_tag = Bytes.new(8)

    def consumer : Consumer?
      @consumer
    end

    def initialize(session : Session, name : String, remote_handle : UInt32,
                   local_handle : UInt32, @queue : LavinMQ::AMQP::Queue, dynamic_queue : LavinMQ::AMQP::Queue? = nil)
      super(session, name, remote_handle, local_handle, Role::Sender, dynamic_queue)
      consumer = Consumer.new(self, @queue)
      @consumer = consumer
      @queue.add_consumer(consumer)
    end

    def add_credit(link_credit : UInt32, delivery_count : UInt32?) : Nil
      if delivery_count
        sent = @delivery_count &- delivery_count
        @credit = link_credit > sent ? link_credit - sent : 0_u32
      else
        @credit = link_credit
      end
      @credit_available.set(@credit > 0)
      ensure_deliver_loop
    end

    def accepts? : Bool
      !closed? && @credit > 0
    end

    def ensure_deliver_loop
      return if closed?
      return if @deliver_loop_running.swap(true, :acquire_release)
      @queue.deliver_loop_wg.spawn(name: "AMQP10 sender link #{@queue.vhost.name}/#{@queue.name}") { deliver_loop }
    end

    private def deliver_loop
      loop do
        wait_for_credit
        wait_for_queue
        break if closed?
        delivered = @queue.consume_get(false) do |env|
          delivery_id = @session.next_delivery_id
          @delivery_count &+= 1
          @credit &-= 1 if @credit > 0
          @credit_available.set(@credit > 0)
          IO::ByteFormat::NetworkEndian.encode(delivery_id.to_u64, @delivery_tag)
          @unacked << Unack.new(delivery_id, @queue, env.segment_position, RoughTime.instant)
          unless @session.client.send_transfer(@session, self, delivery_id, @delivery_tag, env.message)
            close
          end
          @session.increment_deliver_count(env.redelivered)
        end
        unless delivered
          @credit_available.set(@credit > 0)
          Fiber.yield
        end
      end
    rescue ex : LavinMQ::AMQP::Queue::ClosedError | IO::Error | ::Channel::ClosedError
      @session.client.@log.debug { "AMQP 1.0 sender link deliver loop exited: #{ex.inspect}" }
    ensure
      @deliver_loop_running.set(false, :release)
      ensure_deliver_loop if !closed? && @credit > 0 && !@queue.empty?
    end

    private def wait_for_credit
      until closed? || @credit > 0
        select
        when @credit_available.when_true.receive
        when @closed_channel.receive
        end
      end
    end

    private def wait_for_queue
      while !closed? && @queue.empty?
        select
        when @queue.empty.when_false.receive
        when @closed_channel.receive
        end
      end
    end

    def settle(first : UInt32, last : UInt32, outcome : Outcome) : Nil
      found = false
      @unacked.reject! do |unack|
        next false unless first <= unack.delivery_id <= last
        found = true
        case outcome
        in .accepted?
          unack.queue.ack(unack.sp)
          @session.increment_ack_count
        in .released?
          unack.queue.reject(unack.sp, requeue: true)
          @session.increment_reject_count
        in .rejected?, .modified?
          unack.queue.reject(unack.sp, requeue: false)
          @session.increment_reject_count
        end
        true
      end
      @credit_available.set(@credit > 0) if found
    end

    def close : Nil
      return if closed?
      @closed = true
      @credit_available.close
      @closed_channel.close
      @consumer.try &.close
      @unacked.each do |unack|
        unack.queue.reject(unack.sp, requeue: true)
      end
      @unacked.clear
      super
    end
  end

  class Consumer < LavinMQ::Client::Channel::Consumer
    getter tag, priority = 0, prefetch_count = 0_u16
    getter? exclusive = false
    getter? no_ack = false
    getter queue
    getter has_capacity = BoolChannel.new(true)
    getter? closed = false

    def initialize(@link : SenderLink, @queue : LavinMQ::AMQP::Queue)
      @tag = "amqp10.ctag-#{Random::Secure.urlsafe_base64(12)}"
    end

    def ensure_deliver_loop
      @link.ensure_deliver_loop
    end

    def accepts? : Bool
      @link.accepts?
    end

    def flow(active : Bool)
    end

    def close
      return if @closed
      @closed = true
      @queue.rm_consumer(self)
    end

    def cancel
      close
    end

    def ack(sp)
    end

    def reject(sp, requeue = false)
    end

    def deliver(msg, sp, redelivered = false, recover = false)
    end

    def unacked
      0
    end

    def prefetch_count=(value)
      @prefetch_count = value.to_u16
    end

    def unacked_messages
      [] of UnackedMessage
    end

    def details_tuple
      {
        queue: {
          name:  @queue.name,
          vhost: @queue.vhost.name,
        },
        consumer_tag:   @tag,
        exclusive:      false,
        ack_required:   true,
        prefetch_count: @prefetch_count,
        priority:       0,
      }
    end
  end

  class Session < LavinMQ::Client::Channel
    include Stats
    include SortableJSON

    getter client, id, name
    property? running = true
    @links = Hash(UInt32, Link).new
    @sender_links = Array(SenderLink).new
    @next_local_handle = 0_u32
    @next_outgoing_id = 0_u32
    @visited = Set(Exchange).new
    @found_queues = Set(Queue).new

    rate_stats({"ack", "publish", "deliver", "redeliver", "reject"})

    def initialize(@client : Client, @id : UInt16)
      @name = "#{@client.connection_info.remote_address}[#{@id}]"
    end

    def details_tuple
      {
        number:                  @id,
        name:                    @name,
        vhost:                   @client.vhost.name,
        user:                    @client.user.name,
        consumer_count:          @sender_links.size,
        prefetch_count:          0,
        global_prefetch_count:   0,
        confirm:                 false,
        transactional:           false,
        messages_unacknowledged: 0,
        connection_details:      @client.connection_details,
        state:                   @running ? "running" : "closed",
        message_stats:           current_stats_details,
      }
    end

    def prefetch_count=(value)
    end

    def consumers_size : Int32
      @sender_links.size
    end

    def consumers : Array(LavinMQ::Client::Channel::Consumer)
      result = [] of LavinMQ::Client::Channel::Consumer
      @sender_links.each do |link|
        if consumer = link.consumer
          result << consumer
        end
      end
      result
    end

    def find_consumer(& : LavinMQ::Client::Channel::Consumer -> Bool) : LavinMQ::Client::Channel::Consumer?
      consumers.find { |consumer| yield consumer }
    end

    def check_consumer_timeout
    end

    def attach(frame : Attach) : Nil
      local_handle = next_local_handle
      case frame.role
      in .sender?
        target = attach_receiver(frame, local_handle)
        link = ReceiverLink.new(self, frame.name, frame.handle, local_handle, target[0], target[1])
        @links[frame.handle] = link
        @client.send_attach(self, link, frame.source, target[2])
        @client.send_flow(self, link, UInt32::MAX)
      in .receiver?
        source = attach_sender(frame, local_handle)
        link = SenderLink.new(self, frame.name, frame.handle, local_handle, source[0], source[1])
        @links[frame.handle] = link
        @sender_links << link
        @client.send_attach(self, link, source[2], frame.target)
      end
    rescue ex : ProtocolError
      @client.@log.warn { "AMQP 1.0 attach rejected: #{ex.message}" }
      @client.send_detach(self, frame.handle, true,
        ErrorInfo.new(ErrorCondition::PRECONDITION_FAILED, ex.message))
    end

    private def attach_receiver(frame, local_handle)
      target = frame.target || Target.new(nil)
      reject_unsupported_terminus!(target)
      if target.dynamic
        raise ProtocolError.new("dynamic target address must be empty") if target.address
        q = @client.declare_dynamic_queue
        address = "/queues/#{q.name}"
        parsed = PublishAddress.new("", q.name)
        {parsed, q, Target.new(address, dynamic: true)}
      elsif address = target.address
        parsed = @client.resolve_publish_target(address)
        {parsed, nil, target}
      else
        {nil, nil, target}
      end
    end

    private def attach_sender(frame, local_handle)
      source = frame.source || raise ProtocolError.new("source required")
      reject_unsupported_terminus!(source)
      if source.filter
        raise ProtocolError.new("source filters are not supported")
      end
      if source.dynamic
        raise ProtocolError.new("dynamic source address must be empty") if source.address
        q = @client.declare_dynamic_queue
        address = "/queues/#{q.name}"
        {q, q, Source.new(address, dynamic: true)}
      elsif address = source.address
        q = @client.resolve_source(address)
        {q, nil, source}
      else
        raise ProtocolError.new("source address required")
      end
    end

    private def reject_unsupported_terminus!(terminus : Source | Target) : Nil
      raise ProtocolError.new("durable termini are not supported") unless terminus.durable.zero?
      raise ProtocolError.new("dynamic-node-properties are not supported") if terminus.dynamic_node_properties
      if address = terminus.address
        raise ProtocolError.new("management links are not supported") if address.includes?("$management")
      end
    end

    def flow(frame : Flow) : Nil
      if handle = frame.handle
        link = @links[handle]? || raise ProtocolError.new("unknown link handle #{handle}")
        case link
        when SenderLink
          link.add_credit(frame.link_credit || 0_u32, frame.delivery_count)
        when ReceiverLink
          # Client-side credit on a sender-to-server link is irrelevant.
        end
      end
    end

    def flow(active : Bool)
    end

    def transfer(transfer : TransferCodec::TransferView, payload : Bytes) : Nil
      link = @links[transfer.handle]? || raise ProtocolError.new("unknown link handle #{transfer.handle}")
      receiver = link.as?(ReceiverLink) || raise ProtocolError.new("transfer sent on non-receiver link")
      receiver.receive(transfer, payload)
    end

    def disposition(frame : TransferCodec::DispositionView) : Nil
      return unless frame.role.receiver?
      last = frame.last || frame.first
      outcome = frame.outcome || Outcome::Accepted
      @sender_links.each(&.settle(frame.first, last, outcome))
    end

    def detach(frame : Detach) : Nil
      if link = @links.delete(frame.handle)
        @sender_links.delete(link)
        link.close
        @client.send_detach(self, link.local_handle, frame.closed)
      end
    end

    def publish(msg : Message) : LavinMQ::AMQP::Exchange::PublishResult
      @publish_count.add(1, :relaxed)
      @client.vhost.event_tick(EventType::ClientPublish)
      @client.vhost.publish(msg, false, @visited, @found_queues)
    end

    def next_delivery_id : UInt32
      delivery_id = @next_outgoing_id
      @next_outgoing_id &+= 1
      delivery_id
    end

    def increment_deliver_count(redelivered : Bool)
      if redelivered
        @redeliver_count.add(1, :relaxed)
        @client.vhost.event_tick(EventType::ClientRedeliver)
      else
        @deliver_count.add(1, :relaxed)
        @client.vhost.event_tick(EventType::ClientDeliver)
      end
    end

    def increment_ack_count
      @ack_count.add(1, :relaxed)
      @client.vhost.event_tick(EventType::ClientAck)
    end

    def increment_reject_count
      @reject_count.add(1, :relaxed)
      @client.vhost.event_tick(EventType::ClientReject)
    end

    def close
      return unless @running
      @running = false
      @links.each_value &.close
      @links.clear
      @sender_links.clear
      @client.vhost.event_tick(EventType::ChannelClosed)
    end

    private def next_local_handle : UInt32
      @next_local_handle &+= 1
    end
  end
end
