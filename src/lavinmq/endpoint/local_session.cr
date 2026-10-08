require "./session"
require "./local_consumer"
require "./local_stream_consumer"
require "../persister"
require "../message"
require "../event_type"
require "../rough_time"
require "../config"
require "../name_validator"

module LavinMQ
  class VHost; end # vhost.cr requires this file

  module Endpoint
    # A session with a vhost of this broker, run in-process, see `Session`.
    #
    # It isn't a client connection: there's no socket and no user. Operations
    # go straight to the vhost, and are authorized when the shovel or
    # federation upstream that uses the URI is created.
    class LocalSession < Session
      include Persister::ConfirmTarget

      record Unack, tag : UInt64, consumer : LocalConsumer, sp : SegmentPosition

      @vhost : VHost?
      @closed = true
      @prefetch = 0_u16
      @next_tag = 0_u64
      @consumers = Hash(String, LocalConsumer).new
      # Deliveries not yet acked or rejected, in tag order
      @unacked = Deque(Unack).new
      @unacked_lock = Mutex.new
      # Publishes waiting for their confirm, in publish order
      @unconfirmed = Deque(Tuple(UInt64, Proc(Bool, Nil))).new
      @confirm_lock = Mutex.new
      @confirm_seq = 0_u64
      # Durable confirms from the persister; the latest is all that matters
      # since they are cumulative (see AMQP::Channel#enqueue_confirm_ack)
      @confirm_mailbox = ::Channel(UInt64).new(1)
      @closed_signal = ::Channel(Nil).new
      # True while no publish is waiting for its confirm
      @all_confirmed = BoolChannel.new(true)

      # `origin` is the vhost the shovel or federation link belongs to; the
      # session's vhost, `vhost_name`, is looked up from it when opened.
      def initialize(@origin : VHost, @vhost_name : String, name : String)
        super(name)
      end

      def open : Nil
        vhost = @origin.sibling(@vhost_name)
        raise Error.new("vhost '#{@vhost_name}' not found") if vhost.nil? || vhost.closed?
        @vhost = vhost
        next_generation
        @closed = false
        # Like a new AMQP channel, a reopened session tags deliveries from 1
        @next_tag = 0_u64
        @closed_signal = ::Channel(Nil).new
        @confirm_mailbox = ::Channel(UInt64).new(1)
        mailbox = @confirm_mailbox
        signal = @closed_signal
        spawn(confirm_loop(mailbox), name: "#{@name} confirms")
        spawn(watch_vhost(vhost, signal), name: "#{@name} vhost watcher")
      end

      def close : Nil
        do_close
      end

      def closed? : Bool
        @closed
      end

      private def do_close : Bool
        return false if @closed
        @closed = true
        @closed_signal.close
        @confirm_mailbox.close
        # A snapshot: consuming fibers remove their own consumer as they end
        consumers = @consumers.values
        @consumers.clear
        consumers.each do |c|
          c.cancel
          c.queue.rm_consumer(c)
        end
        # Like a closed AMQP channel: everything unacked goes back to its queue
        unacked = @unacked_lock.synchronize do
          list = @unacked.to_a
          @unacked.clear
          list
        end
        unacked.each { |u| u.consumer.settled(u.sp, ack: false, requeue: true) }
        # and every publish still waiting for its confirm is nacked
        unconfirmed = @confirm_lock.synchronize do
          list = @unconfirmed.to_a
          @unconfirmed.clear
          @all_confirmed.set(true)
          list
        end
        unconfirmed.each { |(_, cb)| cb.call(false) }
        true
      end

      private def watch_vhost(vhost, signal)
        select
        when vhost.closed.when_true.receive?
          notify_closed("vhost '#{vhost.name}' closed") if do_close
        when signal.receive?
        end
      end

      private def vhost : VHost
        raise ClosedError.new("Session #{@name} is closed") if @closed
        @vhost || raise ClosedError.new("Session #{@name} not open")
      end

      def declare_queue(name : String, passive : Bool, durable = true, auto_delete = false,
                        args = AMQ::Protocol::Table.new) : Tuple(String, UInt32)
        v = vhost
        if q = v.queue?(name)
          raise Refused.new("403 - queue '#{name}' in vhost '#{v.name}' is internal") if q.internal?
          raise Refused.new("405 - queue '#{name}' in vhost '#{v.name}' is exclusive") if q.exclusive?
          q.redeclare
          return {q.name, q.message_count}
        end
        raise NotFound.new("404 - no queue '#{name}' in vhost '#{v.name}'") if passive
        validate_new_name!(name, "queue")
        name = AMQP::Queue.generate_name if name.empty?
        raise Refused.new("403 - queue limit in vhost '#{v.name}' is reached") if v.queue_limit_reached?
        v.declare_queue(name, durable, auto_delete, args)
        {name, 0_u32}
      rescue ex : LavinMQ::Error::PreconditionFailed
        raise Refused.new("406 - #{ex.message}")
      end

      def declare_exchange(name : String, type : String, passive : Bool,
                           args = AMQ::Protocol::Table.new) : Nil
        v = vhost
        if v.exchange?(name)
          return
        end
        raise NotFound.new("404 - no exchange '#{name}' in vhost '#{v.name}'") if passive
        validate_new_name!(name, "exchange")
        raise Refused.new("403 - can't declare the default exchange") if name.empty?
        v.declare_exchange(name, type, true, false, arguments: args)
      rescue ex : LavinMQ::Error::PreconditionFailed | LavinMQ::Error::ExchangeTypeError
        raise Refused.new("406 - #{ex.message}")
      end

      private def validate_new_name!(name, kind)
        return if name.empty? && kind == "queue"
        unless NameValidator.valid_entity_name?(name)
          raise Refused.new("406 - #{kind} name '#{name}' isn't valid")
        end
        if NameValidator.reserved_prefix?(name)
          raise Refused.new("403 - #{kind} name prefix #{NameValidator::PREFIX_LIST} is reserved")
        end
      end

      def delete_queue(name : String) : Nil
        vhost.delete_queue(name)
      end

      def delete_exchange(name : String) : Nil
        vhost.delete_exchange(name)
      end

      def bind_queue(queue : String, exchange : String, routing_key : String,
                     args = AMQ::Protocol::Table.new) : Nil
        v = vhost
        raise NotFound.new("404 - no queue '#{queue}' in vhost '#{v.name}'") unless v.queue?(queue)
        raise NotFound.new("404 - no exchange '#{exchange}' in vhost '#{v.name}'") unless v.exchange?(exchange)
        v.bind_queue(queue, exchange, routing_key, args)
      end

      def bind_exchange(destination : String, source : String, routing_key : String,
                        args = AMQ::Protocol::Table.new) : Nil
        v = vhost
        raise NotFound.new("404 - no exchange '#{source}' in vhost '#{v.name}'") unless v.exchange?(source)
        raise NotFound.new("404 - no exchange '#{destination}' in vhost '#{v.name}'") unless v.exchange?(destination)
        v.bind_exchange(destination, source, routing_key, args)
      end

      def unbind_exchange(destination : String, source : String, routing_key : String,
                          args = AMQ::Protocol::Table.new) : Nil
        vhost.unbind_exchange(destination, source, routing_key, args)
      end

      def prefetch=(count : UInt16)
        @prefetch = count
      end

      def consume(queue : String, tag : String, no_ack : Bool, exclusive : Bool,
                  args : AMQ::Protocol::Table, &blk : Delivery -> Nil) : Nil
        v = vhost
        q = v.queue?(queue) || raise NotFound.new("404 - no queue '#{queue}' in vhost '#{v.name}'")
        raise Refused.new("403 - queue '#{queue}' in vhost '#{v.name}' is internal") if q.internal?
        raise Refused.new("405 - queue '#{queue}' in vhost '#{v.name}' is exclusive") if q.exclusive?
        tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}" if tag.empty?
        raise Refused.new("530 - consumer tag '#{tag}' in use") if @consumers.has_key?(tag)
        consumer =
          if q.is_a?(AMQP::Stream)
            LocalStreamConsumer.new(self, q, tag, no_ack, @prefetch, args)
          else
            if q.in_exclusive_use?(exclusive)
              raise Refused.new("403 - queue '#{queue}' in vhost '#{v.name}' in exclusive use")
            end
            LocalConsumer.new(self, q, tag, no_ack, exclusive, @prefetch)
          end
        @consumers[tag] = consumer
        q.add_consumer(consumer)
        begin
          consumer.run(&blk)
        ensure
          if @consumers.delete(tag)
            consumer.cancel
            q.rm_consumer(consumer)
          end
        end
        raise ClosedError.new("Session #{@name} closed") if @closed
      end

      def cancel(tag : String) : Nil
        @consumers[tag]?.try &.cancel
      end

      # Called by a consumer for each delivery: assigns its delivery tag and,
      # unless it's no-ack, tracks it until it's settled. Returns nil once the
      # session is closed: checked under the lock #close snapshots the unacked
      # deliveries with, so a delivery is either requeued by the close or
      # refused here, never left behind.
      # :nodoc:
      def next_delivery_tag(consumer : LocalConsumer, sp : SegmentPosition) : UInt64?
        @unacked_lock.synchronize do
          return if @closed
          tag = @next_tag += 1
          @unacked.push(Unack.new(tag, consumer, sp)) unless consumer.no_ack?
          tag
        end
      end

      def ack(delivery_tag : UInt64, multiple = false) : Nil
        settle(delivery_tag, multiple, ack: true, requeue: false)
      end

      def reject(delivery_tag : UInt64, requeue : Bool) : Nil
        settle(delivery_tag, false, ack: false, requeue: requeue)
      end

      private def settle(delivery_tag, multiple, ack, requeue) : Nil
        @unacked_lock.synchronize do
          if multiple
            while (u = @unacked.first?) && u.tag <= delivery_tag
              @unacked.shift
              u.consumer.settled(u.sp, ack, requeue)
            end
          elsif idx = @unacked.bsearch_index { |u| u.tag >= delivery_tag }
            u = @unacked[idx]
            if u.tag == delivery_tag
              @unacked.delete_at(idx)
              u.consumer.settled(u.sp, ack, requeue)
            end
          end
        end
      end

      def publish(exchange : String, routing_key : String,
                  properties : AMQ::Protocol::Properties, body : Bytes) : Nil
        publish(exchange, routing_key, properties, body, false, nil)
      end

      def publish(exchange : String, routing_key : String,
                  properties : AMQ::Protocol::Properties, body : Bytes,
                  &on_confirm : Bool -> Nil) : Nil
        publish(exchange, routing_key, properties, body, false, on_confirm)
      end

      # Publishes like an AMQP client does, but returns how the message was
      # routed. With `immediate` the message is only routed to queues with a
      # consumer ready to take it, and nacked if there's none.
      def publish(exchange : String, routing_key : String,
                  properties : AMQ::Protocol::Properties, body : Bytes,
                  immediate : Bool, on_confirm : Proc(Bool, Nil)?) : AMQP::Exchange::PublishResult
        v = vhost
        raise Refused.new("406 - Server low on disk space") unless v.flow?
        ex = v.exchange?(exchange) || raise NotFound.new("404 - no exchange '#{exchange}' in vhost '#{v.name}'")
        raise Refused.new("403 - exchange '#{exchange}' in vhost '#{v.name}' is internal") if ex.internal?
        if body.bytesize > Config.instance.max_message_size
          raise Refused.new("406 - message size #{body.bytesize} larger than max size #{Config.instance.max_message_size}")
        end
        if properties.timestamp_raw.nil? && Config.instance.set_timestamp?
          properties.timestamp = RoughTime.utc
        end
        msg = Message.new(RoughTime.unix_ms, exchange, routing_key, properties,
          body.bytesize.to_u64, IO::Memory.new(body, writable: false))
        v.event_tick(EventType::ClientPublish)
        return v.publish(msg, immediate) unless on_confirm
        msg.needs_sync = true
        seq = @confirm_lock.synchronize do
          s = @confirm_seq += 1
          @unconfirmed.push({s, on_confirm})
          @all_confirmed.set(false)
          s
        end
        result = begin
          v.publish(msg, immediate)
        rescue err
          nack(seq)
          raise err
        end
        # A message a queue refused (reject-publish overflow), or that no
        # consumer was ready for when published `immediate`, isn't delivered
        if result.overflowed? || (immediate && !result.routed?)
          nack(seq)
        else
          v.enqueue_ack(self, seq)
        end
        result
      end

      # Waits until every publish so far is confirmed (or nacked), at most
      # `timeout`. Returns false on timeout.
      def wait_for_confirms(timeout : Time::Span) : Bool
        select
        when @all_confirmed.when_true.receive?
          true
        when timeout(timeout)
          false
        end
      end

      private def nack(seq)
        cb = @confirm_lock.synchronize do
          idx = @unconfirmed.index { |(s, _)| s == seq } || return
          entry = @unconfirmed.delete_at(idx)
          @all_confirmed.set(true) if @unconfirmed.empty?
          entry[1]
        end
        cb.call(false)
      end

      # Called from the persister once the publishes up to `msgid` are durable
      def enqueue_confirm_ack(msgid : UInt64) : Nil
        mailbox = @confirm_mailbox
        loop do
          return if mailbox.try_send(msgid)
          mailbox.try_receive?
        end
      rescue ::Channel::ClosedError
      end

      private def confirm_loop(mailbox)
        while msgid = mailbox.receive?
          loop do
            cb = @confirm_lock.synchronize do
              first = @unconfirmed.first? || break
              break if first[0] > msgid
              entry = @unconfirmed.shift
              @all_confirmed.set(true) if @unconfirmed.empty?
              entry[1]
            end
            break unless cb
            cb.call(true)
          end
        end
      end
    end
  end
end
