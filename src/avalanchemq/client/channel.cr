require "logger"
require "./channel/consumer"
require "../queue"
require "../amqp"
require "../stats"
require "../sortable_json"

module AvalancheMQ
  abstract class Client
    class Channel
      include Stats
      include SortableJSON

      getter id, client, prefetch_size, prefetch_count, global_prefetch,
        confirm, log, consumers, name
      property? running = true
      getter? client_flow

      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @next_msg_body = IO::Memory.new(4096)
      @log : Logger
      @client_flow = true

      rate_stats(%w(ack get publish deliver redeliver reject confirm return_unroutable))
      property deliver_count, redeliver_count

      def initialize(@client : Client, @id : UInt16)
        @log = @client.log.dup
        @log.progname += " channel=#{@id}"
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        @prefetch_size = 0_u32
        @prefetch_count = 0_u16
        @confirm_total = 0_u64
        @confirm = false
        @global_prefetch = false
        @next_publish_mandatory = false
        @next_publish_immediate = false

        @consumers = Array(Consumer).new
        @delivery_tag = 0_u64
        @unacked = Deque(Unack).new
        @unack_lock = Mutex.new
      end

      record Unack,
        tag : UInt64,
        queue : Queue,
        sp : SegmentPosition,
        persistent : Bool,
        consumer : Consumer?

      def details_tuple
        {
          number:                  @id,
          name:                    @name,
          vhost:                   @client.vhost.name,
          user:                    @client.user.try(&.name),
          consumer_count:          @consumers.size,
          prefetch_count:          @prefetch_count,
          global_prefetch_count:   @global_prefetch ? @prefetch_count : 0,
          confirm:                 @confirm,
          transactional:           false,
          messages_unacknowledged: @unacked.size,
          connection_details:      @client.connection_details,
          state:                   state,
          message_stats:           stats_details,
        }
      end

      def client_flow(active : Bool)
        @client_flow = active
        return unless active
        @consumers.each { |c| c.queue.consumer_available }
      end

      def state
        !@running ? "closed" : (@client.vhost.flow? ? "running" : "flow")
      end

      def send(frame)
        if @running
          @client.send frame
        else
          @log.debug { "Channel is closed so is not sending #{frame.inspect}" }
        end
      end

      def confirm_select(frame)
        @confirm = true
        unless frame.no_wait
          send AMQP::Frame::Confirm::SelectOk.new(frame.channel)
        end
      end

      def start_publish(frame)
        unless server_flow?
          @client.send_precondition_failed(frame, "Server low on disk space")
          return
        end
        @next_publish_exchange_name = frame.exchange
        @next_publish_routing_key = frame.routing_key
        @next_publish_mandatory = frame.mandatory
        @next_publish_immediate = frame.immediate
        unless @client.vhost.exchanges[@next_publish_exchange_name]?
          msg = "No exchange '#{@next_publish_exchange_name}' in vhost '#{@client.vhost.name}'"
          @client.send_not_found(frame, msg)
        end
      end

      MAX_MESSAGE_BODY_SIZE = 512 * 1024 * 1024

      def next_msg_headers(frame)
        if direct_reply_request?(frame.properties.reply_to)
          if @client.direct_reply_channel
            frame.properties.reply_to = "#{DIRECT_REPLY_PREFIX}.#{@client.direct_reply_consumer_tag}"
          else
            @client.send_precondition_failed(frame, "Direct reply consumer does not exist")
            return
          end
        end
        if frame.body_size > MAX_MESSAGE_BODY_SIZE
          error = "message size #{frame.body_size} larger than max size #{MAX_MESSAGE_BODY_SIZE}"
          @client.send_precondition_failed(frame, error)
          return
        end
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(@next_msg_body) if frame.body_size.zero?
      end

      def add_content(frame)
        copied = IO.copy(frame.body, @next_msg_body, frame.body_size)
        if copied != frame.body_size
          raise IO::Error.new("Could only copy #{copied} of #{frame.body_size} bytes")
        end
        if @next_msg_body.pos == @next_msg_size
          @next_msg_body.rewind
          finish_publish(@next_msg_body)
        end
      end

      private def server_flow?
        @client.vhost.flow?
      end

      private def finish_publish(message_body)
        @publish_count += 1
        ts = Time.utc
        props = @next_msg_props.not_nil!
        props.timestamp = ts if Config.instance.set_timestamp && props.timestamp.nil?
        msg = Message.new(ts.to_unix_ms,
          @next_publish_exchange_name.not_nil!,
          @next_publish_routing_key.not_nil!,
          props,
          @next_msg_size,
          message_body)
        publish_and_return(msg)
      rescue ex
        @log.warn { "Could not handle message #{ex.inspect}" }
        confirm_nack
        raise ex
      ensure
        @next_msg_size = 0_u64
        @next_msg_body.clear
        @next_msg_props = nil
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
      end

      private def publish_and_return(msg)
        return true if direct_reply?(msg)
        @confirm_total += 1 if @confirm
        ok = @client.vhost.publish msg, immediate: @next_publish_immediate
        if ok
          @client.vhost.waiting4confirm(self) if @confirm
        else
          basic_return(msg)
        end
        Fiber.yield if @publish_count % 8192 == 0
      rescue Queue::RejectOverFlow
        confirm_nack
      end

      def confirm_nack(multiple = false)
        return unless @confirm
        @confirm_count += 1 # Stats
        send AMQP::Frame::Basic::Nack.new(@id, @confirm_total, multiple, requeue: false)
      end

      private def direct_reply?(msg) : Bool
        return false unless msg.routing_key.starts_with?(DIRECT_REPLY_PREFIX)
        consumer_tag = msg.routing_key.lchop("#{DIRECT_REPLY_PREFIX}.")
        @client.vhost.direct_reply_channels[consumer_tag]?.try do |ch|
          deliver = AMQP::Frame::Basic::Deliver.new(ch.id, consumer_tag,
            1_u64, false,
            msg.exchange_name,
            msg.routing_key)
          ch.deliver(deliver, msg)
          return true
        end
        false
      end

      def confirm_ack(multiple = false)
        return unless @confirm
        @confirm_count += 1 # Stats
        send AMQP::Frame::Basic::Ack.new(@id, @confirm_total, multiple)
      end

      private def basic_return(msg)
        @return_unroutable_count += 1
        if @next_publish_immediate
          retrn = AMQP::Frame::Basic::Return.new(@id, 313_u16, "NO_CONSUMERS", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
        elsif @next_publish_mandatory
          retrn = AMQP::Frame::Basic::Return.new(@id, 312_u16, "NO_ROUTE", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
        end
        # basic.nack will only be delivered if an internal error occurs...
        confirm_ack
      end

      def deliver(frame, msg)
        @client.deliver(frame, msg)
      end

      def consume(frame)
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        if direct_reply_request?(frame.queue)
          unless frame.no_ack
            @client.send_precondition_failed(frame, "Direct replys must be consumed in no-ack mode")
            return
          end
          @log.debug { "Saving direct reply consumer #{frame.consumer_tag}" }
          @client.direct_reply_consumer_tag = frame.consumer_tag
          @client.vhost.direct_reply_channels[frame.consumer_tag] = self
          unless frame.no_wait
            send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
          end
        elsif q = @client.vhost.queues[frame.queue]? || nil
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
            return
          end
          if q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
            return
          end
          unless frame.no_wait
            send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
          end
          c = Consumer.new(self, frame.consumer_tag, q, frame.no_ack, frame.exclusive)
          @consumers.push(c)
          q.add_consumer(c)
        else
          @client.send_not_found(frame, "Queue '#{frame.queue}' not declared")
        end
        Fiber.yield # Notify :add_consumer observers
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if q.exclusive && !@client.exclusive_queues.includes? q
            @client.send_resource_locked(frame, "Exclusive queue")
          else
            q.basic_get(frame.no_ack) do |env|
              if env
                persistent = env.message.properties.delivery_mode == 2_u8
                delivery_tag = next_delivery_tag(q, env.segment_position,
                                                 persistent, frame.no_ack,
                                                 nil)
                get_ok = AMQP::Frame::Basic::GetOk.new(frame.channel, delivery_tag,
                  env.redelivered, env.message.exchange_name,
                  env.message.routing_key, q.message_count)
                deliver(get_ok, env.message)
                @redeliver_count += 1 if env.redelivered
              else
                send AMQP::Frame::Basic::GetEmpty.new(frame.channel)
              end
            end
          end
          @get_count += 1
        else
          @client.send_not_found(frame, "No queue '#{frame.queue}' in vhost '#{@client.vhost.name}'")
          close
        end
      end

      # Find one (more multiple) unacked delivery tags, yielding each item
      # Returns true if found at least one item, else false
      private def delete_unacked(delivery_tag, multiple = false, &blk : Unack -> _) : Bool
        found = false
        @unack_lock.synchronize do
          if multiple
            while unack = @unacked.shift?
              if delivery_tag.zero? || unack.tag <= delivery_tag
                found = true
                yield unack
              else
                @unacked.unshift unack
                break
              end
            end
          else
            # optimization for acking first unacked
            if @unacked[0]?.try(&.tag) == delivery_tag
              @log.debug { "Unacked found tag at front" }
              found = true
              yield @unacked.shift
            else
              # @unacked is always sorted so can do a binary search
              idx = @unacked.bsearch_index { |ua, _| ua.tag >= delivery_tag }
              @log.debug { "Bsearch of unacked found tag at idx #{idx}" }
              if idx
                found = true
                yield @unacked.delete_at idx
              end
            end
          end
        end
        found
      end

      def basic_ack(frame)
        found = delete_unacked(frame.delivery_tag, frame.multiple) do |unack|
          do_ack(unack)
        end
        unless found
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      private def do_ack(unack)
        if c = unack.consumer
          c.ack(unack.sp)
        end
        unack.queue.ack(unack.sp, persistent: unack.persistent)
        @ack_count += 1
      end

      def basic_reject(frame)
        found = delete_unacked(frame.delivery_tag, false) do |unack|
          do_reject(frame.requeue, unack)
        end
        unless found
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      def basic_nack(frame)
        found = delete_unacked(frame.delivery_tag, frame.multiple) do |unack|
          do_reject(frame.requeue, unack)
        end
        unless found
          reply_text = "Unknown delivery tag '#{frame.delivery_tag}'"
          @client.send_precondition_failed(frame, reply_text)
        end
      end

      private def do_reject(requeue, unack)
        if c = unack.consumer
          c.reject(unack.sp)
        end
        unack.queue.reject(unack.sp, requeue)
        @reject_count += 1
      end

      def basic_qos(frame)
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        send AMQP::Frame::Basic::QosOk.new(frame.channel)
      end

      def basic_recover(frame)
        @consumers.each { |c| c.recover(frame.requeue) }
        delete_unacked(0_u64, multiple: true) do |unack|
          unack.queue.reject(unack.sp, true) if unack.consumer.nil?
        end
        send AMQP::Frame::Basic::RecoverOk.new(frame.channel)
      end

      def close
        @running = false
        @consumers.each { |c| c.queue.rm_consumer(c) }
        delete_unacked(0_u64, multiple: true) do |unack|
          unack.queue.reject(unack.sp, true) if unack.consumer.nil?
        end
        @log.debug { "Closed" }
      end

      def next_delivery_tag(queue : Queue, sp, persistent, no_ack, consumer) : UInt64
        @unack_lock.synchronize do
          tag = @delivery_tag += 1
          unless no_ack
            @unacked.push Unack.new(tag, queue, sp, persistent, consumer)
          end
          tag
        end
      end

      def recover(consumer)
        @unacked.delete_if do |unack|
          if unack.consumer == consumer
            yield unack.sp
            true
          end
        end
      end

      def cancel_consumer(frame)
        @log.debug { "Cancelling consumer '#{frame.consumer_tag}'" }
        if c = @consumers.find { |cons| cons.tag == frame.consumer_tag }
          c.queue.rm_consumer(c)
          unless frame.no_wait
            send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        else
          unless frame.no_wait
            send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
          end
        end
      end

      DIRECT_REPLY_PREFIX = "amq.direct.reply-to"

      def direct_reply_request?(str)
        # no regex for speed
        str.try { |r| r == "amq.rabbitmq.reply-to" || r.starts_with? DIRECT_REPLY_PREFIX }
      end
    end
  end
end
