require "../stats"
require "./client"
require "./io_memory_reset"

module LavinMQ::AMQP10
  abstract class Link
    getter session, name, remote_handle, local_handle, role, dynamic_queue
    @closed = Atomic(Bool).new(false)
    @delivery_count = Atomic(UInt32).new(0_u32)

    def initialize(@session : Session, @name : String, @remote_handle : UInt32,
                   @local_handle : UInt32, @role : Role, @dynamic_queue : LavinMQ::AMQP::Queue? = nil)
    end

    def closed? : Bool
      @closed.get(:acquire)
    end

    def delivery_count : UInt32
      @delivery_count.get(:acquire)
    end

    def close : Nil
      return unless close_once
      close_dynamic_queue
    end

    private def close_once : Bool
      !@closed.swap(true, :acquire_release)
    end

    private def increment_delivery_count : Nil
      @delivery_count.add(1_u32, :acquire_release)
    end

    private def advance_delivery_count(by : UInt32) : Nil
      @delivery_count.add(by, :acquire_release) if by > 0
    end

    private def close_dynamic_queue : Nil
      @session.client.close_dynamic_queue(@dynamic_queue)
    end
  end

  class ReceiverLink < Link
    @body_io = IO::Memory.new(Bytes.empty)
    @message_reader = IO::Memory.new(Bytes.empty)
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
      if (fmt = transfer.message_format) && fmt != 0_u32
        raise ProtocolError.new("unsupported message-format #{fmt}")
      end
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

      # Match the 0-9-1 publish path: refuse to write while flow control is off
      # (e.g. low disk space). Release so the sender can retry later.
      unless @session.client.vhost.flow?
        settle(delivery_id, settled, Outcome::Released)
        return
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
    rescue ex : ProtocolError | DecodeError | LavinMQ::Error::PreconditionFailed | OverflowError
      @session.client.log.warn { "AMQP 1.0 publish rejected: #{ex.message}" }
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

    getter queue
    getter delivery_tag_buffer : Bytes
    @consumer : Consumer?
    @unacked = Deque(Unack).new
    @unack_lock = Mutex.new(:checked)
    @credit = 0_u32
    @credit_lock = Mutex.new(:checked)
    @drain = Atomic(Bool).new(false)
    @deliver_loop_running = Atomic(Bool).new(false)
    @credit_available = BoolChannel.new(false)
    @closed_channel = ::Channel(Nil).new
    @delivery_tag_buffer = Bytes.new(8)
    # snd-settle-mode == 1 (settled): deliver pre-settled, no dispositions expected.
    @settled = false

    def consumer : Consumer?
      @consumer
    end

    def initialize(session : Session, name : String, remote_handle : UInt32,
                   local_handle : UInt32, @queue : LavinMQ::AMQP::Queue,
                   dynamic_queue : LavinMQ::AMQP::Queue? = nil, snd_settle_mode : UInt8? = nil)
      super(session, name, remote_handle, local_handle, Role::Sender, dynamic_queue)
      @settled = snd_settle_mode == 1_u8
      consumer = Consumer.new(self, @queue)
      @consumer = consumer
      @queue.add_consumer(consumer)
    end

    def settled? : Bool
      @settled
    end

    # Called by the client under the connection write lock, so recording the
    # unacked delivery and writing the transfer stay ordered together.
    def record_unacked(delivery_id : UInt32, sp : SegmentPosition) : Nil
      @unack_lock.synchronize do
        @unacked << Unack.new(delivery_id, @queue, sp, RoughTime.instant)
      end
    end

    def detach_from_server(error : ErrorInfo? = nil) : Nil
      @session.detach_link(self, error)
    end

    def credit : UInt32
      @credit_lock.synchronize { @credit }
    end

    def add_credit(link_credit : UInt32, delivery_count : UInt32?, drain : Bool = false, echo : Bool = false) : Nil
      has_credit = @credit_lock.synchronize do
        if delivery_count
          sent = self.delivery_count &- delivery_count
          @credit = link_credit > sent ? link_credit - sent : 0_u32
        else
          @credit = link_credit
        end
        @credit > 0
      end
      @drain.set(drain)
      @credit_available.set(has_credit)
      set_consumer_capacity(has_credit)
      if has_credit
        # The deliver loop delivers messages and, in drain mode, echoes a drained
        # flow once the queue runs dry. A plain echo is answered here directly.
        ensure_deliver_loop
        @session.client.send_flow(@session, self, credit) if echo && !drain
      elsif drain
        # No credit left to consume, so the drain is already complete: echo a flow
        # advertising zero credit so the receiver stops waiting.
        @session.client.send_flow(@session, self, 0_u32, drain: true)
      elsif echo
        @session.client.send_flow(@session, self, credit)
      end
    end

    private def drain? : Bool
      @drain.get(:acquire)
    end

    private def complete_drain : Nil
      @credit_lock.synchronize do
        advance_delivery_count(@credit)
        @credit = 0_u32
      end
      @drain.set(false)
      @credit_available.set(false)
      set_consumer_capacity(false)
      @session.client.send_flow(@session, self, 0_u32, drain: true)
    end

    def accepts? : Bool
      !closed? && @session.client.vhost.flow? && credit > 0
    end

    def ensure_deliver_loop
      return if closed?
      return if @session.client.closed?
      return if @deliver_loop_running.swap(true, :acquire_release)
      @queue.deliver_loop_wg.spawn(name: "AMQP10 sender link #{@queue.vhost.name}/#{@queue.name}") { deliver_loop }
    end

    private def deliver_loop
      wait_for_single_active_consumer
      loop do
        wait_for_credit
        deliver_one if wait_until_deliverable
      end
    rescue IdleTimeout
      # Idle with credit on an empty queue: exit the fiber and let the queue
      # respawn it on the next publish, matching the 0-9-1 consumer.
      @session.client.log.debug { "AMQP 1.0 sender link deliver loop idle, exiting" }
    rescue ex : ClosedError | LavinMQ::AMQP::Queue::ClosedError | IO::Error | ::Channel::ClosedError
      @session.client.log.debug { "AMQP 1.0 sender link deliver loop exited: #{ex.inspect}" }
    ensure
      @deliver_loop_running.set(false, :release)
      # Do not respawn when the connection is gone: a failed transfer closes the
      # socket, and respawning here would busy-loop and, worse, re-add a fiber to
      # the queue's deliver-loop wait-group while it is being torn down.
      ensure_deliver_loop if !closed? && !@session.client.closed? && credit > 0 && !@queue.empty?
    end

    # Blocks until a message can be delivered. Returns false (after completing the
    # drain) when drain mode catches the queue empty, so no message should be sent.
    private def wait_until_deliverable : Bool
      loop do
        raise ClosedError.new if closed?
        next if wait_for_priority_consumers
        # In drain mode an empty queue must not block: the remaining credit is
        # consumed by advancing delivery-count instead of waiting for messages.
        if drain? && @queue.empty?
          complete_drain
          return false
        end
        wait_for_queue
        raise ClosedError.new if closed?
        next if wait_for_paused_queue
        next if wait_for_flow
        next if wait_for_remote_window
        # Credit may have been revoked (flow with link-credit 0) while we waited
        # for a message; block for credit again rather than over-deliver.
        if credit.zero?
          wait_for_credit
          next
        end
        return true
      end
    end

    private def deliver_one : Nil
      settled = @settled
      delivered = @queue.consume_get(settled) do |env|
        increment_delivery_count
        has_credit = decrement_credit
        @credit_available.swap(has_credit)
        set_consumer_capacity(has_credit)
        # send_transfer assigns the delivery-id and records the unacked entry
        # under the connection write lock so ids stay ordered across links.
        unless @session.client.send_transfer(@session, self, env.message, env.segment_position, settled)
          # The transfer failed and the socket is closed; break the loop and let
          # connection cleanup requeue and detach, avoiding a self-close deadlock
          # on the queue's deliver-loop wait-group.
          raise ClosedError.new
        end
        @session.increment_deliver_count(env.redelivered)
      end
      if delivered
        # All requested credit was consumed by real transfers; echo the drained
        # flow so the receiver knows the drain is complete.
        complete_drain if drain? && credit.zero?
      elsif drain?
        complete_drain
      else
        has_credit = credit > 0
        @credit_available.swap(has_credit)
        set_consumer_capacity(has_credit)
        Fiber.yield
      end
    end

    private def wait_for_single_active_consumer
      active = @queue.single_active_consumer
      return if active.nil? || active == @consumer
      loop do
        select
        when sac = @queue.single_active_consumer_change.receive
          break if sac == @consumer
        when @closed_channel.receive
          raise ClosedError.new
        end
      end
      true
    end

    private def wait_for_priority_consumers
      # Single-active-consumer queues choose exactly one consumer instead of using priorities.
      if @queue.has_priority_consumers? && @queue.single_active_consumer.nil?
        priority = @consumer.try(&.priority) || 0
        higher_prio_consumers = @queue.consumers.select { |c| c.priority > priority }
        return false unless higher_prio_consumers.any? &.accepts?
        loop do
          wait_channels = Array(::Channel(Nil)).new(higher_prio_consumers.size + 1)
          higher_prio_consumers.each { |consumer| wait_channels << consumer.has_capacity.when_false }
          wait_channels << @closed_channel
          ::Channel.receive_first(wait_channels)
          raise ClosedError.new if closed?
          break
        rescue ::Channel::ClosedError
          raise ClosedError.new if closed?
          higher_prio_consumers = @queue.consumers.select { |c| c.priority > priority }
          break if higher_prio_consumers.empty?
        end
        return true
      end
    end

    private def wait_for_credit
      until closed? || credit > 0
        @session.client.flush # push buffered deliveries before we block
        select
        when @credit_available.when_true.receive
        when @closed_channel.receive
        end
      end
    end

    private def wait_for_queue
      while !closed? && @queue.empty?
        @session.client.flush
        select
        when @queue.empty.when_false.receive
        when @closed_channel.receive
        when timeout Config.instance.deliver_loop_idle_timeout
          raise IdleTimeout.new
        end
      end
    end

    private def wait_for_paused_queue
      if @queue.state.paused?
        @session.client.flush
        select
        when @queue.paused.when_false.receive
        when @closed_channel.receive
          raise ClosedError.new
        end
        return true
      end
    end

    private def wait_for_flow
      unless @session.client.vhost.flow?
        @session.client.flush
        select
        when @session.client.vhost.flow_change.when_true.receive
        when @closed_channel.receive
          raise ClosedError.new
        end
        return true
      end
    end

    # Blocks until the peer's session incoming-window has room for another
    # transfer, so we never overrun the window the client advertised.
    private def wait_for_remote_window
      unless @session.remote_window_open?
        @session.client.flush
        select
        when @session.remote_window.when_true.receive
        when @closed_channel.receive
          raise ClosedError.new
        end
        return true
      end
    end

    private def decrement_credit : Bool
      @credit_lock.synchronize do
        @credit &-= 1 if @credit > 0
        @credit > 0
      end
    end

    def timed_out? : Bool
      @unack_lock.synchronize do
        unack = @unacked.first?
        return false unless unack
        timeout = unack.queue.consumer_timeout
        return false unless timeout
        RoughTime.instant - unack.delivered_at > timeout.milliseconds
      end
    end

    def unacked : Int32
      @unack_lock.synchronize { @unacked.size }
    end

    private def set_consumer_capacity(has_capacity : Bool) : Nil
      @consumer.try &.has_capacity.swap(has_capacity)
    end

    def settle(first : UInt32, last : UInt32, outcome : Outcome) : Nil
      found = false
      @unack_lock.synchronize do
        # @unacked is sorted ascending by delivery-id, so skip links whose range
        # does not overlap and stop scanning once past `last`.
        next if @unacked.empty?
        next if last < @unacked.first.delivery_id || first > @unacked.last.delivery_id
        i = 0
        while i < @unacked.size
          unack = @unacked[i]
          break if unack.delivery_id > last
          unless first <= unack.delivery_id
            i += 1
            next
          end
          found = true
          case outcome
          in .accepted?
            unack.queue.ack(unack.sp)
            @session.increment_ack_count
          in .released?, .modified?
            unack.queue.reject(unack.sp, requeue: true)
            @session.increment_reject_count
          in .rejected?
            unack.queue.reject(unack.sp, requeue: false)
            @session.increment_reject_count
          end
          @unacked.delete_at(i)
        end
      end
      if found
        @credit_available.swap(credit > 0)
        set_consumer_capacity(credit > 0)
      end
    end

    def close : Nil
      return unless close_once
      @credit_available.close
      @closed_channel.close
      @unack_lock.synchronize do
        @unacked.each do |unack|
          unack.queue.reject(unack.sp, requeue: true)
        end
        @unacked.clear
      end
      @consumer.try &.close
      close_dynamic_queue
    end

    class ClosedError < Error; end

    # Raised to unwind the deliver loop after it has sat idle with credit; the
    # queue respawns it on the next publish.
    class IdleTimeout < Exception; end
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
      @has_capacity.close
      @queue.rm_consumer(self)
    end

    def cancel
      @link.detach_from_server(ErrorInfo.new(ErrorCondition::RESOURCE_DELETED, "resource deleted"))
    end

    def ack(sp)
    end

    def reject(sp, requeue = false)
    end

    def deliver(msg, sp, redelivered = false, recover = false)
    end

    def unacked
      @link.unacked
    end

    def prefetch_count=(value)
      @prefetch_count = value.to_u16
    end

    def unacked_messages
      [] of UnackedMessage
    end

    def details_tuple
      channel_details = @link.session.details_tuple
      {
        queue: {
          name:  @queue.name,
          vhost: @queue.vhost.name,
        },
        consumer_tag:    @tag,
        exclusive:       false,
        ack_required:    true,
        prefetch_count:  @prefetch_count,
        priority:        0,
        channel_details: {
          peer_host:       channel_details[:connection_details][:peer_host],
          peer_port:       channel_details[:connection_details][:peer_port],
          connection_name: channel_details[:connection_details][:name],
          user:            channel_details[:user],
          number:          channel_details[:number],
          name:            channel_details[:name],
        },
      }
    end
  end

  class Session < LavinMQ::Client::Channel
    include Stats
    include SortableJSON

    INCOMING_WINDOW_REFILL_AT = DEFAULT_WINDOW // 2

    getter client, id, name
    getter remote_window : BoolChannel
    property? running = true
    @links = Hash(UInt32, Link).new
    @sender_links = Array(SenderLink).new
    @next_local_handle = 0_u32
    # Our outgoing transfer-id counter; a delivery-id equals the transfer-id of
    # the delivery's first frame, so this doubles as the delivery-id source.
    @next_outgoing_id = Atomic(UInt32).new(0_u32)
    @next_incoming_id : Atomic(UInt32)
    @incoming_window_remaining = Atomic(UInt32).new(DEFAULT_WINDOW)
    # How many transfers the peer's session incoming-window can still accept.
    @remote_incoming_window : Atomic(UInt32)
    @visited = Set(Exchange).new
    @found_queues = Set(AMQP::Queue).new

    rate_stats({"ack", "publish", "deliver", "redeliver", "reject"})

    def initialize(@client : Client, @id : UInt16, begin_frame : Begin)
      @name = "#{@client.connection_info.remote_address}[#{@id}]"
      # Seed our incoming transfer-id from the peer's initial next-outgoing-id and
      # track the session window it advertised.
      @next_incoming_id = Atomic(UInt32).new(begin_frame.next_outgoing_id)
      @remote_incoming_window = Atomic(UInt32).new(begin_frame.incoming_window)
      @remote_window = BoolChannel.new(begin_frame.incoming_window > 0)
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
        messages_unacknowledged: unacked_count,
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

    def unacked_count : Int32
      @sender_links.sum(&.unacked)
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
      @sender_links.dup.each do |link|
        next unless link.timed_out?
        detach_link(link, ErrorInfo.new(ErrorCondition::PRECONDITION_FAILED, "consumer timeout"))
        break
      end
    end

    # Server-initiated link teardown: notify the peer with a detach and drop the
    # link from the session maps. Used by consumer-timeout and queue deletion.
    def detach_link(link : SenderLink, error : ErrorInfo? = nil) : Nil
      @links.delete(link.remote_handle)
      @sender_links.delete(link)
      @client.send_detach(self, link.local_handle, true, error)
      link.close
    end

    def attach(frame : Attach) : Nil
      attach(frame, next_local_handle)
    end

    private def attach(frame : Attach, local_handle : UInt32) : Nil
      if @links.has_key?(frame.handle)
        raise ProtocolError.new("handle #{frame.handle} is already in use")
      end
      case frame.role
      in .sender?
        target = attach_receiver(frame, local_handle)
        link = ReceiverLink.new(self, frame.name, frame.handle, local_handle, target[0], target[1])
        @links[frame.handle] = link
        @client.send_attach(self, link, frame.source, target[2], frame)
        @client.send_flow(self, link, Int32::MAX.to_u32)
      in .receiver?
        source = attach_sender(frame, local_handle)
        link = SenderLink.new(self, frame.name, frame.handle, local_handle, source[0], source[1], frame.snd_settle_mode)
        @links[frame.handle] = link
        @sender_links << link
        @client.send_attach(self, link, source[2], frame.target, frame)
      end
    rescue ex : ProtocolError
      @client.log.warn { "AMQP 1.0 attach rejected: #{ex.message}" }
      @client.send_rejected_attach(self, frame, local_handle)
      @client.send_detach(self, local_handle, true,
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
      update_remote_window(frame)
      if handle = frame.handle
        link = @links[handle]? || raise ProtocolError.new("unknown link handle #{handle}")
        case link
        when SenderLink
          link.add_credit(frame.link_credit || 0_u32, frame.delivery_count, frame.drain, frame.echo)
        when ReceiverLink
          # We grant the sender effectively unlimited credit at attach, so the
          # client's credit accounting is irrelevant; only honour an echo request.
          @client.send_flow(self, link, Int32::MAX.to_u32) if frame.echo
        end
      elsif frame.echo
        # Session-level echo request: reply with the current session flow state.
        @client.send_session_flow(self)
      end
    end

    def flow(active : Bool)
      @sender_links.each do |link|
        link.consumer.try &.flow(active)
      end
    end

    def transfer(transfer : TransferCodec::TransferView, payload : Bytes) : Nil
      advance_incoming_window
      link = @links[transfer.handle]? || raise ProtocolError.new("unknown link handle #{transfer.handle}")
      receiver = link.as?(ReceiverLink) || raise ProtocolError.new("transfer sent on non-receiver link")
      receiver.receive(transfer, payload)
    end

    def disposition(frame : TransferCodec::DispositionView) : Nil
      return unless frame.role.receiver?
      outcome = frame.outcome
      if outcome.nil?
        # A missing/non-terminal delivery state (e.g. received) must not be
        # treated as acceptance. Only a bare settlement with no state at all is
        # taken as accepted.
        return if !frame.settled || frame.state_present
        outcome = Outcome::Accepted
      end
      last = frame.last || frame.first
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

    def next_outgoing_id : UInt32
      @next_outgoing_id.get(:acquire)
    end

    # Advance the outgoing transfer-id by the number of frames just written and
    # consume that many from the peer's session incoming-window. Called by the
    # client under the write lock, right after emitting a transfer.
    def advance_outgoing(frames : UInt32) : Nil
      @next_outgoing_id.add(frames, :acquire_release)
      loop do
        current = @remote_incoming_window.get(:acquire)
        remaining = current > frames ? current - frames : 0_u32
        _, exchanged = @remote_incoming_window.compare_and_set(current, remaining)
        if exchanged
          @remote_window.swap(remaining > 0)
          break
        end
      end
    end

    def remote_window_open? : Bool
      @remote_incoming_window.get(:acquire) > 0
    end

    private def update_remote_window(frame : Flow) : Nil
      return unless niw = frame.incoming_window
      nid = frame.next_incoming_id || @next_outgoing_id.get(:acquire)
      # window = peer.next-incoming-id + peer.incoming-window - our next-outgoing-id
      window = nid.to_i64 + niw.to_i64 - @next_outgoing_id.get(:acquire).to_i64
      window = 0_i64 if window < 0
      clamped = window > UInt32::MAX ? UInt32::MAX : window.to_u32
      @remote_incoming_window.set(clamped, :release)
      @remote_window.swap(clamped > 0)
    end

    def next_incoming_id : UInt32
      @next_incoming_id.get(:acquire)
    end

    def incoming_window : UInt32
      @incoming_window_remaining.get(:acquire)
    end

    private def advance_incoming_window : Nil
      # next-incoming-id counts transfer frames, so advance once per received
      # frame (multi-frame deliveries advance it per continuation).
      @next_incoming_id.add(1_u32, :acquire_release)
      remaining = decrement_incoming_window
      if remaining <= INCOMING_WINDOW_REFILL_AT
        @incoming_window_remaining.set(DEFAULT_WINDOW, :release)
        @client.send_session_flow(self)
      end
    end

    private def decrement_incoming_window : UInt32
      loop do
        current = @incoming_window_remaining.get(:acquire)
        remaining = current > 0 ? current - 1 : 0_u32
        _, exchanged = @incoming_window_remaining.compare_and_set(current, remaining)
        return remaining if exchanged
      end
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
      handle = @next_local_handle
      @next_local_handle &+= 1
      handle
    end
  end
end
