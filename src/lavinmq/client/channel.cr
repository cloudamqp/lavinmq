require "./channel/consumer"
require "../queue"
require "../exchange"
require "../amqp"
require "../stats"
require "../sortable_json"
require "../error"

module LavinMQ
  class Client
    class Channel
      include Stats
      include SortableJSON

      getter id, client, prefetch_size, prefetch_count,
        confirm, log, consumers, name
      property? running = true
      getter? flow = true, global_prefetch = false

      @next_publish_mandatory = false
      @next_publish_immediate = false
      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @log : Log
      @prefetch_size = 0_u32
      @prefetch_count = 0_u16
      @confirm_total = 0_u64
      @confirm = false
      @global_prefetch = false
      @consumers = Array(Consumer).new
      @delivery_tag = 0_u64
      @unacked = Deque(Unack).new
      @unack_lock = Mutex.new(:checked)
      @next_msg_body_file : File?
      @direct_reply_consumer : String?

      rate_stats(%w(ack get publish deliver redeliver reject confirm return_unroutable))
      property deliver_count, redeliver_count

      def initialize(@client : Client, @id : UInt16)
        @log = Log.for "channel[client=#{@client.remote_address} id=#{@id}]"
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        @client.vhost.event_tick(EventType::ChannelCreated)
        @next_msg_body_tmp = IO::Memory.new
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

      def flow(active : Bool)
        @flow = active
        @consumers.each &.flow(active)
        send AMQP::Frame::Channel::FlowOk.new(@id, active)
      end

      def state
        !@running ? "closed" : @flow ? "running" : "flow"
      end

      def send(frame)
        unless @running
          @log.debug { "Channel is closed so is not sending #{frame.inspect}" }
          return false
        end
        @client.send frame, true
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
        ex = @client.vhost.exchanges[@next_publish_exchange_name]?
        unless ex
          msg = "No exchange '#{@next_publish_exchange_name}' in vhost '#{@client.vhost.name}'"
          @client.send_not_found(frame, msg)
        end
        if ex && ex.internal
          msg = "Exchange '#{@next_publish_exchange_name}' in vhost '#{@client.vhost.name}' is internal"
          @client.send_access_refused(frame, msg)
        end
      end

      private def direct_reply_to?(str) : Bool
        {"amq.rabbitmq.reply-to", "amq.direct.reply-to"}.includes? str
      end

      def next_msg_headers(frame, ts)
        raise Error::UnexpectedFrame.new(frame) if @next_publish_exchange_name.nil?
        raise Error::UnexpectedFrame.new(frame) if frame.class_id != 60
        valid_expiration?(frame) || return
        if direct_reply_to?(frame.properties.reply_to)
          if drc = @direct_reply_consumer
            frame.properties.reply_to = "amq.direct.reply-to.#{drc}"
          else
            @client.send_precondition_failed(frame, "Direct reply consumer does not exist")
            return
          end
        end
        if frame.body_size > Config.instance.max_message_size
          error = "message size #{frame.body_size} larger than max size #{Config.instance.max_message_size}"
          @client.send_precondition_failed(frame, error)
          @log.warn { "Message size exceeded, #{frame.body_size}/#{Config.instance.max_message_size}" }
          return
        end
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(@next_msg_body_tmp, ts) if frame.body_size.zero?
      end

      def add_content(frame, ts)
        if @next_publish_exchange_name.nil? || @next_msg_props.nil?
          frame.body.skip(frame.body_size)
          raise Error::UnexpectedFrame.new(frame)
        end
        if frame.body_size == @next_msg_size
          IO.copy(frame.body, @next_msg_body_tmp, frame.body_size)
          @next_msg_body_tmp.rewind
          begin
            finish_publish(@next_msg_body_tmp, ts)
          ensure
            @next_msg_body_tmp.clear
          end
        else
          copied = IO.copy(frame.body, next_msg_body_file, frame.body_size)
          if copied != frame.body_size
            raise IO::Error.new("Could only copy #{copied} of #{frame.body_size} bytes")
          end
          if next_msg_body_file.pos == @next_msg_size
            next_msg_body_file.rewind
            begin
              finish_publish(next_msg_body_file, ts)
            ensure
              next_msg_body_file.truncate
              next_msg_body_file.rewind
            end
          end
        end
      end

      private def valid_expiration?(frame) : Bool
        if exp = frame.properties.expiration
          if i = exp.to_i?
            if i < 0
              @client.send_precondition_failed(frame, "Negative expiration not allowed")
              return false
            end
          else
            @client.send_precondition_failed(frame, "Expiration not a number")
            return false
          end
        end
        true
      end

      private def server_flow?
        @client.vhost.flow?
      end

      private def finish_publish(body_io, ts)
        @publish_count += 1
        @client.vhost.event_tick(EventType::ClientPublish)
        props = @next_msg_props.not_nil!
        props.timestamp ||= ts if Config.instance.set_timestamp
        msg = Message.new(ts.to_unix * 1000,
          @next_publish_exchange_name.not_nil!,
          @next_publish_routing_key.not_nil!,
          props,
          @next_msg_size,
          body_io)
        publish_and_return(msg)
      rescue ex
        unless ex.is_a? IO::Error
          @log.warn { "Error when publishing message #{ex.inspect}" }
        end
        confirm_nack
        raise ex
      ensure
        @next_msg_size = 0_u64
        @next_msg_props = nil
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
      end

      @visited = Set(Exchange).new
      @found_queues = Set(Queue).new

      private def publish_and_return(msg) # ameba:disable Metrics/CyclomaticComplexity
        return true if direct_reply?(msg)
        if user_id = msg.properties.user_id
          if user_id != @client.user.name && !@client.user.can_impersonate?
            text = "Message's user_id property '#{user_id}' doesn't match actual user '#{@client.user.name}'"
            @log.error { text }
            raise Error::PreconditionFailed.new(text)
          end
        end
        @confirm_total += 1 if @confirm
        ok = @client.vhost.publish msg, @next_publish_immediate, @visited, @found_queues, @confirm
        if ok
          @client.vhost.waiting4confirm(self) if @confirm
        else
          basic_return(msg)
        end
      rescue e : Error::PreconditionFailed
        msg.body_io.skip(msg.size)
        send AMQP::Frame::Channel::Close.new(@id, 406_u16, "PRECONDITION_FAILED - #{e.message}", 60_u16, 40_u16)
      rescue Queue::RejectOverFlow
        confirm_nack
      rescue Queue::Closed
        confirm_nack
      end

      def confirm_nack(multiple = false)
        return unless @confirm
        @client.vhost.event_tick(EventType::ClientPublishConfirm)
        @confirm_count += 1 # Stats
        send AMQP::Frame::Basic::Nack.new(@id, @confirm_total, multiple, requeue: false)
      end

      private def direct_reply?(msg) : Bool
        return false unless msg.routing_key.starts_with? "amq.direct.reply-to."
        consumer_tag = msg.routing_key[20..]
        if ch = @client.vhost.direct_reply_consumers[consumer_tag]?
          deliver = AMQP::Frame::Basic::Deliver.new(ch.id, consumer_tag,
            1_u64, false,
            msg.exchange_name,
            msg.routing_key)
          ch.deliver(deliver, msg)
          @confirm_total += 1 if @confirm
          confirm_ack
          true
        else
          false
        end
      end

      def confirm_ack(multiple = false)
        return unless @confirm
        @client.vhost.event_tick(EventType::ClientPublishConfirm)
        @confirm_count += 1 # Stats
        send AMQP::Frame::Basic::Ack.new(@id, @confirm_total, multiple)
      end

      def basic_return(msg)
        @return_unroutable_count += 1
        if @next_publish_immediate
          retrn = AMQP::Frame::Basic::Return.new(@id, 313_u16, "NO_CONSUMERS", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
        elsif @next_publish_mandatory
          retrn = AMQP::Frame::Basic::Return.new(@id, 312_u16, "NO_ROUTE", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
        else
          @log.debug { "Skipping body of non read message #{msg.body_io.class}" }
          unless msg.body_io.is_a?(File)
            msg.body_io.skip(msg.size)
          end
        end
        # basic.nack will only be delivered if an internal error occurs...
        confirm_ack
      end

      def deliver(frame, msg, redelivered = false) : Bool
        unless @running
          @log.debug { "Channel is closed so is not sending #{frame.inspect}" }
          return false
        end
        # Make sure publishes are confirmed before we deliver
        # can happend if the vhost spawned fsync fiber has not execed yet
        @client.vhost.send_publish_confirms
        @client.deliver(frame, msg) || return false
        if redelivered
          @redeliver_count += 1
          @client.vhost.event_tick(EventType::ClientRedeliver)
        else
          @deliver_count += 1
          @client.vhost.event_tick(EventType::ClientDeliver)
        end
        true
      end

      def consume(frame)
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        if direct_reply_to?(frame.queue)
          unless frame.no_ack
            @client.send_precondition_failed(frame, "Direct replys must be consumed in no-ack mode")
            return
          end
          @log.debug { "Saving direct reply consumer #{frame.consumer_tag}" }
          @direct_reply_consumer = frame.consumer_tag
          @client.vhost.direct_reply_consumers[frame.consumer_tag] = self
          unless frame.no_wait
            send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
          end
        elsif q = @client.vhost.queues[frame.queue]?
          if @client.queue_exclusive_to_other_client?(q)
            @client.send_resource_locked(frame, "Exclusive queue")
            return
          end
          if q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
            return
          end
          if q.internal?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' is internal")
            return
          end
          priority = consumer_priority(frame) # Must be before ConsumeOk, can close channel
          unless frame.no_wait
            send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
          end
          c = Consumer.new(self, frame.consumer_tag, q, frame.no_ack, frame.exclusive, priority)
          @consumers.push(c)
          q.add_consumer(c)
        else
          @client.send_not_found(frame, "Queue '#{frame.queue}' not declared")
        end
        Fiber.yield # Notify :add_consumer observers
      end

      private def consumer_priority(frame) : Int32
        priority = 0
        if frame.arguments.has_key? "x-priority"
          prio_arg = frame.arguments["x-priority"]
          prio_int = prio_arg.as?(Int) || raise Error::PreconditionFailed.new("x-priority must be an integer")
          begin
            priority = prio_int.to_i
          rescue OverflowError
            raise Error::PreconditionFailed.new("x-priority out of bounds, must fit a 32-bit integer")
          end
        end
        priority
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if @client.queue_exclusive_to_other_client?(q)
            @client.send_resource_locked(frame, "Exclusive queue")
          elsif q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
          elsif q.internal?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' is internal")
          else
            @get_count += 1
            @client.vhost.event_tick(EventType::ClientGet)
            q.basic_get(frame.no_ack) do |env|
              persistent = env.message.properties.delivery_mode == 2_u8
              delivery_tag = next_delivery_tag(q, env.segment_position,
                persistent, frame.no_ack,
                nil)
              get_ok = AMQP::Frame::Basic::GetOk.new(frame.channel, delivery_tag,
                env.redelivered, env.message.exchange_name,
                env.message.routing_key, q.message_count)
              deliver(get_ok, env.message, env.redelivered)
              return
            end
            send AMQP::Frame::Basic::GetEmpty.new(frame.channel)
          end
        else
          @client.send_not_found(frame, "No queue '#{frame.queue}' in vhost '#{@client.vhost.name}'")
        end
      end

      private def delete_unacked(delivery_tag) : Unack?
        @unack_lock.synchronize do
          # @unacked is always sorted so can do a binary search
          # optimization for acking first unacked
          if @unacked[0]?.try(&.tag) == delivery_tag
            @log.debug { "Unacked found tag:#{delivery_tag} at front" }
            @unacked.shift
          elsif idx = @unacked.bsearch_index { |unack, _| unack.tag >= delivery_tag }
            return nil unless @unacked[idx].tag == delivery_tag
            @log.debug { "Unacked bsearch found tag:#{delivery_tag} at index:#{idx}" }
            @unacked.delete_at(idx)
          end
        end
      end

      private def delete_multiple_unacked(delivery_tag)
        @unack_lock.synchronize do
          if delivery_tag.zero?
            until @unacked.empty?
              yield @unacked.shift
            end
            return
          end
          idx = @unacked.bsearch_index { |unack, _| unack.tag >= delivery_tag }
          return nil unless idx
          return nil unless @unacked[idx].tag == delivery_tag
          @log.debug { "Unacked bsearch found tag:#{delivery_tag} at index:#{idx}" }
          (idx + 1).times do
            yield @unacked.shift
          end
        end
      end

      def basic_ack(frame)
        found = false
        if frame.multiple
          delete_multiple_unacked(frame.delivery_tag) do |unack|
            found = true
            do_ack(unack)
          end
        elsif unack = delete_unacked(frame.delivery_tag)
          found = true
          do_ack(unack)
        end
        unless found
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
        end
      end

      private def do_ack(unack)
        if c = unack.consumer
          c.ack(unack.sp)
        end
        unack.queue.ack(unack.sp)
        @client.vhost.event_tick(EventType::ClientAck)
        @ack_count += 1
      end

      def basic_reject(frame)
        @log.debug { "rejecting #{frame.inspect}" }
        if unack = delete_unacked(frame.delivery_tag)
          do_reject(frame.requeue, unack)
        else
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
        end
        @log.debug { "done rejecting" }
      end

      def basic_nack(frame)
        found = false
        if frame.multiple
          delete_multiple_unacked(frame.delivery_tag) do |unack|
            found = true
            do_reject(frame.requeue, unack)
          end
        elsif unack = delete_unacked(frame.delivery_tag)
          found = true
          do_reject(frame.requeue, unack)
        end
        unless found
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
        end
      end

      private def unknown_tag(delivery_tag)
        # Lower case u important for bunny on_error callback
        "unknown delivery tag #{delivery_tag}"
      end

      private def do_reject(requeue, unack)
        if c = unack.consumer
          c.reject(unack.sp)
        end
        unack.queue.reject(unack.sp, requeue)
        @reject_count += 1
        @client.vhost.event_tick(EventType::ClientReject)
      end

      def basic_qos(frame) : Nil
        @client.send_not_implemented(frame) if frame.prefetch_size != 0
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        send AMQP::Frame::Basic::QosOk.new(frame.channel)
      end

      def basic_recover(frame) : Nil
        @unack_lock.synchronize do
          if frame.requeue
            @unacked.each do |unack|
              if consumer = unack.consumer
                consumer.reject(unack.sp)
              end
              unack.queue.reject(unack.sp, requeue: true)
            end
            @unacked.clear
          else # redeliver to the original recipient
            @unacked.each do |unack|
              if consumer = unack.consumer
                # FIXME: not if the consumer is cancelled
                env = unack.queue.read(unack.sp)
                consumer.deliver(env.message, env.segment_position, true, recover: true)
              else
                unack.queue.reject(unack.sp, requeue: true)
              end
            end
          end
        end
        send AMQP::Frame::Basic::RecoverOk.new(frame.channel)
      end

      def close
        @running = false
        @consumers.each &.close
        @consumers.clear
        if drc = @direct_reply_consumer
          @client.vhost.direct_reply_consumers.delete(drc)
        end
        @unack_lock.synchronize do
          @unacked.each do |unack|
            if unack.consumer.nil? # requeue basic_get msgs
              @log.debug { "Requeing unacked msg #{unack.sp}" }
              unack.queue.reject(unack.sp, true)
            end
          end
          @unacked.clear
        end
        @next_msg_body_file.try &.close
        @client.vhost.event_tick(EventType::ChannelClosed)
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

      def cancel_consumer(frame)
        @log.debug { "Cancelling consumer '#{frame.consumer_tag}'" }
        if idx = @consumers.index { |cons| cons.tag == frame.consumer_tag }
          c = @consumers.delete_at idx
          c.queue.rm_consumer(c, basic_cancel: true)
        elsif @direct_reply_consumer == frame.consumer_tag
          @direct_reply_consumer = nil
          @client.vhost.direct_reply_consumers.delete(frame.consumer_tag)
        end
        unless frame.no_wait
          send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
        end
      end

      private def next_msg_body_file
        @next_msg_body_file ||=
          begin
            tmp_path = File.join(@client.vhost.data_dir, "tmp", Random::Secure.urlsafe_base64)
            File.open(tmp_path, "w+").tap do |f|
              f.sync = true
              f.read_buffering = false
              f.delete
            end
          end
      end
    end
  end
end
