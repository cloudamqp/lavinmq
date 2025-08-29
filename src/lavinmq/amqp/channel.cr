require "../stats"
require "./client"
require "./consumer"
require "./stream_consumer"
require "../error"
require "../logging"
require "./queue"
require "../exchange"
require "../amqp"
require "../sortable_json"
require "./channel_reply_code"
require "../bool_channel"

module LavinMQ
  module AMQP
    class Channel < LavinMQ::Client::Channel
      include Stats
      include SortableJSON
      include LavinMQ::Logging::Loggable

      getter id, name
      property? running = true
      getter? flow = true
      getter consumers = Array(Consumer).new
      getter prefetch_count : UInt16 = Config.instance.default_consumer_prefetch
      getter global_prefetch_count = 0_u16
      getter has_capacity = BoolChannel.new(true)
      getter unacked = Deque(Unack).new
      @basic_get_unacked_count = Atomic(UInt32).new(0)
      @confirm = false
      @confirm_total = 0_u64
      @next_publish_mandatory = false
      @next_publish_immediate = false
      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_props : AMQP::Properties?
      @delivery_tag = Atomic(UInt64).new(1_u64)
      @unack_lock = Mutex.new(:checked)
      @next_msg_body_file : File?
      @direct_reply_consumer : String?
      @tx = false
      @next_msg_body_tmp = IO::Memory.new

      rate_stats({"ack", "get", "get_no_ack", "publish", "deliver", "deliver_no_ack", "deliver_get", "redeliver", "reject", "confirm", "return_unroutable"})

      Log = LavinMQ::Log.for "amqp.channel"

      def initialize(@client : Client, @id : UInt16)
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        L.context(client: @client.connection_info.remote_address.to_s, channel: @id)
      end

      record Unack,
        tag : UInt64,
        queue : Queue,
        sp : SegmentPosition,
        consumer : Consumer?,
        delivered_at : Time::Span

      def details_tuple
        {
          number:                  @id,
          name:                    @name,
          vhost:                   @client.vhost.name,
          user:                    @client.user.name,
          consumer_count:          @consumers.size,
          prefetch_count:          @prefetch_count,
          global_prefetch_count:   @global_prefetch_count,
          confirm:                 @confirm,
          transactional:           @tx,
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
          L.debug "Channel is closed so is not sending frame", frame_type: frame.class.name
          return false
        end
        @client.send frame, true
      end

      def confirm_select(frame)
        if @tx
          @client.send_precondition_failed(frame, "Channel already in transactional mode")
          return
        end
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
        raise LavinMQ::Error::UnexpectedFrame.new(frame) if @next_publish_exchange_name
        if ex = @client.vhost.exchanges[frame.exchange]?
          if !ex.internal?
            @next_publish_exchange_name = frame.exchange
            @next_publish_routing_key = frame.routing_key
            @next_publish_mandatory = frame.mandatory
            @next_publish_immediate = frame.immediate
          else
            @client.send_access_refused(frame, "Exchange '#{frame.exchange}' in vhost '#{@client.vhost.name}' is internal")
          end
        else
          @client.send_not_found(frame, "No exchange '#{frame.exchange}' in vhost '#{@client.vhost.name}'")
        end
      end

      private def direct_reply_to?(str) : Bool
        {"amq.rabbitmq.reply-to", "amq.direct.reply-to"}.includes? str
      end

      def next_msg_headers(frame)
        raise LavinMQ::Error::UnexpectedFrame.new(frame) if @next_publish_exchange_name.nil?
        raise LavinMQ::Error::UnexpectedFrame.new(frame) if @next_msg_props
        raise LavinMQ::Error::UnexpectedFrame.new(frame) if frame.class_id != 60
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
          L.warn "Message size exceeded", body_size: frame.body_size, max_size: Config.instance.max_message_size
          return
        end
        @next_msg_size = frame.body_size
        @next_msg_props = frame.properties
        finish_publish(@next_msg_body_tmp) if frame.body_size.zero?
      end

      @next_msg_body_file_pos = 0

      def add_content(frame)
        if @next_publish_exchange_name.nil? || @next_msg_props.nil?
          frame.body.skip(frame.body_size)
          raise LavinMQ::Error::UnexpectedFrame.new(frame)
        end
        if @tx # in transaction mode, copy all bodies to the tmp file serially
          copied = IO.copy(frame.body, next_msg_body_file, frame.body_size)
          if copied != frame.body_size
            raise IO::Error.new("Could only copy #{copied} of #{frame.body_size} bytes")
          end
          if (@next_msg_body_file_pos += copied) == @next_msg_size
            # as the body_io won't be read until tx_commit there's no need to rewind
            # bodies can be appended sequentially to the tmp file
            finish_publish(next_msg_body_file)
            @next_msg_body_file_pos = 0
          end
        elsif frame.body_size == @next_msg_size
          copied = IO.copy(frame.body, @next_msg_body_tmp, frame.body_size)
          if copied != frame.body_size
            raise IO::Error.new("Could only copy #{copied} of #{frame.body_size} bytes")
          end
          @next_msg_body_tmp.rewind
          begin
            finish_publish(@next_msg_body_tmp)
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
              finish_publish(next_msg_body_file)
            ensure
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

      private def finish_publish(body_io)
        @publish_count.add(1, :relaxed)
        @client.vhost.event_tick(EventType::ClientPublish)
        props = @next_msg_props.not_nil!
        props.timestamp = RoughTime.utc if props.timestamp.nil? && Config.instance.set_timestamp?
        msg = Message.new(RoughTime.unix_ms,
          @next_publish_exchange_name.not_nil!,
          @next_publish_routing_key.not_nil!,
          props,
          @next_msg_size,
          body_io)
        direct_reply?(msg) || publish_and_return(msg)
      ensure
        @next_msg_size = 0_u64
        @next_msg_props = nil
        @next_publish_exchange_name = @next_publish_routing_key = nil
        @next_publish_mandatory = @next_publish_immediate = false
      end

      @visited = Set(Exchange).new
      @found_queues = Set(Queue).new

      record TxMessage, message : Message, mandatory : Bool, immediate : Bool
      @tx_publishes = Array(TxMessage).new

      private def publish_and_return(msg)
        validate_user_id(msg.properties.user_id)
        if @tx
          @tx_publishes.push TxMessage.new(msg, @next_publish_mandatory, @next_publish_immediate)
          return
        end

        confirm do
          ok = @client.vhost.publish msg, @next_publish_immediate, @visited, @found_queues
          basic_return(msg, @next_publish_mandatory, @next_publish_immediate) unless ok
        rescue e : LavinMQ::Error::PreconditionFailed
          msg.body_io.skip(msg.bodysize)
          code = ChannelReplyCode::PRECONDITION_FAILED
          send AMQP::Frame::Channel::Close.new(@id, code.value, "#{code} - #{e.message}", 60_u16, 40_u16)
        end
      rescue Queue::RejectOverFlow
        # nack but then do nothing
      end

      private def validate_user_id(user_id)
        current_user = @client.user
        if user_id && user_id != current_user.name && !current_user.can_impersonate?
          text = "Message's user_id property '#{user_id}' doesn't match actual user '#{current_user.name}'"
          L.error text
          raise LavinMQ::Error::PreconditionFailed.new(text)
        end
      end

      private def confirm(&)
        if @confirm
          msgid = @confirm_total &+= 1
          begin
            yield
            confirm_ack(msgid)
          rescue ex
            confirm_nack(msgid)
            raise ex
          end
        else
          yield
        end
      end

      private def confirm_ack(msgid, multiple = false)
        @client.vhost.event_tick(EventType::ClientPublishConfirm)
        @confirm_count.add(1, :relaxed)
        send AMQP::Frame::Basic::Ack.new(@id, msgid, multiple)
      end

      private def confirm_nack(msgid, multiple = false)
        @client.vhost.event_tick(EventType::ClientPublishConfirm)
        @confirm_count.add(1, :relaxed)
        send AMQP::Frame::Basic::Nack.new(@id, msgid, multiple, requeue: false)
      end

      private def direct_reply?(msg) : Bool
        return false unless msg.routing_key.starts_with? "amq.direct.reply-to."
        consumer_tag = msg.routing_key[20..]
        if ch = @client.vhost.direct_reply_consumers[consumer_tag]?
          confirm do
            deliver = AMQP::Frame::Basic::Deliver.new(ch.id, consumer_tag,
              1_u64, false,
              msg.exchange_name,
              msg.routing_key)
            ch.deliver(deliver, msg)
          end
          true
        else
          false
        end
      end

      private def basic_return(msg : Message, mandatory : Bool, immediate : Bool)
        @return_unroutable_count.add(1, :relaxed)
        if immediate
          retrn = AMQP::Frame::Basic::Return.new(@id, 313_u16, "NO_CONSUMERS", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
          msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
        elsif mandatory
          retrn = AMQP::Frame::Basic::Return.new(@id, 312_u16, "NO_ROUTE", msg.exchange_name, msg.routing_key)
          deliver(retrn, msg)
          msg.body_io.seek(-msg.bodysize.to_i64, IO::Seek::Current) # rewind
        end
      end

      def deliver(frame, msg, redelivered = false, flush = true) : Nil
        raise ClosedError.new("Channel is closed") unless @running
        @client.deliver(frame, msg, flush)
      end

      def increment_deliver_count(redelivered : Bool, no_ack : Bool = false)
        if redelivered
          @redeliver_count.add(1, :relaxed)
          @client.vhost.event_tick(EventType::ClientRedeliver)
        else
          if no_ack
            @deliver_no_ack_count.add(1, :relaxed)
            @client.vhost.event_tick(EventType::ClientDeliverNoAck)
          else
            @deliver_count.add(1, :relaxed)
            @client.vhost.event_tick(EventType::ClientDeliver)
          end
          @deliver_get_count.add(1, :relaxed)
        end
      end

      def consume(frame)
        if @consumers.size >= Config.instance.max_consumers_per_channel > 0
          @client.send_resource_error(frame, "Max #{Config.instance.max_consumers_per_channel} consumers per channel reached")
          return
        end
        if frame.consumer_tag.empty?
          frame.consumer_tag = "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
        end
        if direct_reply_to?(frame.queue)
          unless frame.no_ack
            @client.send_precondition_failed(frame, "Direct replys must be consumed in no-ack mode")
            return
          end
          L.debug "Saving direct reply consumer", consumer_tag: frame.consumer_tag
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
          c = if q.is_a? StreamQueue
                AMQP::StreamConsumer.new(self, q, frame)
              else
                AMQP::Consumer.new(self, q, frame)
              end
          @consumers.push(c)
          q.add_consumer(c)
          unless frame.no_wait
            send AMQP::Frame::Basic::ConsumeOk.new(frame.channel, frame.consumer_tag)
          end
        else
          @client.send_not_found(frame, "Queue '#{frame.queue}' not declared")
        end
        Fiber.yield # Notify :add_consumer observers
      end

      def basic_get(frame)
        if q = @client.vhost.queues.fetch(frame.queue, nil)
          if @client.queue_exclusive_to_other_client?(q)
            @client.send_resource_locked(frame, "Exclusive queue")
          elsif q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
          elsif q.is_a? StreamQueue
            @client.send_not_implemented(frame, "Stream queues does not support basic_get")
          else
            case frame.no_ack
            when true
              @get_no_ack_count.add(1, :relaxed)
              @client.vhost.event_tick(EventType::ClientGetNoAck)
            when false
              @get_count.add(1, :relaxed)
              @client.vhost.event_tick(EventType::ClientGet)
            end
            @deliver_get_count.add(1, :relaxed)
            ok = q.basic_get(frame.no_ack) do |env|
              delivery_tag = next_delivery_tag(q, env.segment_position, frame.no_ack, nil)
              unless frame.no_ack # track unacked messages
                q.basic_get_unacked << UnackedMessage.new(self, delivery_tag, RoughTime.monotonic)
              end
              get_ok = AMQP::Frame::Basic::GetOk.new(frame.channel, delivery_tag,
                env.redelivered, env.message.exchange_name,
                env.message.routing_key, q.message_count)
              deliver(get_ok, env.message, env.redelivered)
            end
            send AMQP::Frame::Basic::GetEmpty.new(frame.channel) unless ok
          end
        else
          @client.send_not_found(frame, "No queue '#{frame.queue}' in vhost '#{@client.vhost.name}'")
        end
      end

      private def delete_unacked(delivery_tag) : Unack?
        found = nil
        notify_has_capacity do
          # @unacked is always sorted so can do a binary search
          # optimization for acking first unacked
          if @unacked[0]?.try(&.tag) == delivery_tag
            # @log.debug { "Unacked found tag:#{delivery_tag} at front" }
            found = @unacked.shift
          elsif idx = @unacked.bsearch_index { |unack, _| unack.tag >= delivery_tag }
            return nil unless @unacked[idx].tag == delivery_tag
            # @log.debug { "Unacked bsearch found tag:#{delivery_tag} at index:#{idx}" }
            found = @unacked.delete_at(idx)
          end
          @basic_get_unacked_count.sub(1u32, :relaxed) if found.try &.consumer.nil?
        end
        found
      end

      private def delete_multiple_unacked(delivery_tag, & : Unack -> Nil)
        notify_has_capacity do
          if delivery_tag.zero?
            until @unacked.empty?
              u = @unacked.shift
              @basic_get_unacked_count.sub(1u32, :relaxed) if u.consumer.nil?
              yield u
            end
          else
            idx = @unacked.bsearch_index { |unack, _| unack.tag >= delivery_tag }
            return nil unless idx
            return nil unless @unacked[idx].tag == delivery_tag
            # @log.debug { "Unacked bsearch found tag:#{delivery_tag} at index:#{idx}" }
            (idx + 1).times do
              u = @unacked.shift
              @basic_get_unacked_count.sub(1u32, :relaxed) if u.consumer.nil?
              yield u
            end
          end
        end
      end

      record TxAck, delivery_tag : UInt64, multiple : Bool, negative : Bool, requeue : Bool
      @tx_acks = Array(TxAck).new

      def basic_ack(frame)
        if @tx
          @unack_lock.synchronize do
            if frame.delivery_tag.zero? && frame.multiple # all msgs so far
              @tx_acks.push(TxAck.new @unacked.last.tag, frame.multiple, false, false)
              return
            elsif @unacked.bsearch { |unack| unack.tag >= frame.delivery_tag }.try &.tag == frame.delivery_tag
              check_double_ack!(frame.delivery_tag)
              @tx_acks.push(TxAck.new frame.delivery_tag, frame.multiple, false, false)
              return
            end
          end
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
          return
        end

        found = false
        if frame.multiple
          found = true if frame.delivery_tag.zero?
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
      rescue DoubleAck
        @client.send_precondition_failed(frame, "Delivery tag already acked")
      end

      private def do_ack(unack)
        if c = unack.consumer
          c.ack(unack.sp)
        end
        unack.queue.ack(unack.sp)
        unack.queue.basic_get_unacked.reject! { |u| u.channel == self && u.delivery_tag == unack.tag }
        @client.vhost.event_tick(EventType::ClientAck)
        @ack_count.add(1, :relaxed)
      end

      def basic_reject(frame)
        if @tx
          @unack_lock.synchronize do
            if @unacked.bsearch { |unack| unack.tag >= frame.delivery_tag }.try &.tag == frame.delivery_tag
              check_double_ack!(frame.delivery_tag)
              @tx_acks.push(TxAck.new frame.delivery_tag, false, true, frame.requeue)
              return
            end
          end
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
          return
        end

        L.debug "Rejecting message", delivery_tag: frame.delivery_tag, requeue: frame.requeue
        if unack = delete_unacked(frame.delivery_tag)
          do_reject(frame.requeue, unack)
        else
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
        end
      rescue DoubleAck
        @client.send_precondition_failed(frame, "Delivery tag already acked")
      end

      def basic_nack(frame)
        if @tx
          @unack_lock.synchronize do
            if frame.delivery_tag.zero? && frame.multiple # all msgs so far
              @tx_acks.push(TxAck.new @unacked.last.tag, true, true, frame.requeue)
              return
            elsif @unacked.bsearch { |unack| unack.tag >= frame.delivery_tag }.try &.tag == frame.delivery_tag
              check_double_ack!(frame.delivery_tag)
              @tx_acks.push(TxAck.new frame.delivery_tag, frame.multiple, true, frame.requeue)
              return
            end
          end
          @client.send_precondition_failed(frame, unknown_tag(frame.delivery_tag))
          return
        end

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
      rescue DoubleAck
        @client.send_precondition_failed(frame, "Delivery tag already acked")
      end

      private class DoubleAck < Error; end

      private def check_double_ack!(delivery_tag)
        if @tx_acks.any? { |tx_ack| tx_ack.delivery_tag == delivery_tag }
          raise DoubleAck.new
        end
      end

      private def unknown_tag(delivery_tag)
        # Lower case u important for bunny on_error callback
        "unknown delivery tag #{delivery_tag}"
      end

      private def do_reject(requeue, unack)
        if c = unack.consumer
          c.reject(unack.sp, requeue)
        end
        unack.queue.reject(unack.sp, requeue)
        unack.queue.basic_get_unacked.reject! { |u| u.channel == self && u.delivery_tag == unack.tag }
        @reject_count.add(1, :relaxed)
        @client.vhost.event_tick(EventType::ClientReject)
      end

      def basic_qos(frame) : Nil
        @client.send_not_implemented(frame) if frame.prefetch_size != 0
        if frame.global
          notify_has_capacity do
            @global_prefetch_count = frame.prefetch_count
          end
        else
          self.prefetch_count = frame.prefetch_count
        end
        send AMQP::Frame::Basic::QosOk.new(frame.channel)
      end

      def prefetch_count=(value)
        @consumers.each(&.prefetch_count = value)
        @prefetch_count = value
      end

      def basic_recover(frame) : Nil
        notify_has_capacity do
          if frame.requeue
            @unacked.each do |unack|
              next if delivery_tag_is_in_tx?(unack.tag)
              if consumer = unack.consumer
                consumer.reject(unack.sp, requeue: true)
              end
              unack.queue.reject(unack.sp, requeue: true)
            end
            @unacked.clear
          else # redeliver to the original recipient
            @unacked.reject! do |unack|
              next if delivery_tag_is_in_tx?(unack.tag)
              if (consumer = unack.consumer) && !consumer.closed?
                env = unack.queue.read(unack.sp)
                consumer.deliver(env.message, env.segment_position, true, recover: true)
                false
              else
                unack.queue.reject(unack.sp, requeue: true)
                true
              end
            end
          end
        end
        send AMQP::Frame::Basic::RecoverOk.new(frame.channel)
      end

      private def delivery_tag_is_in_tx?(delivery_tag) : Bool
        if @tx
          @tx_acks.any? do |tx_ack|
            (tx_ack.delivery_tag > delivery_tag && tx_ack.multiple) || tx_ack.delivery_tag == delivery_tag
          end
        else
          false
        end
      end

      def close
        @running = false
        @consumers.each_with_index(1) do |consumer, i|
          consumer.close
          Fiber.yield if (i % 128) == 0
        end
        @consumers.clear
        if drc = @direct_reply_consumer
          @client.vhost.direct_reply_consumers.delete(drc)
        end
        @unack_lock.synchronize do
          @unacked.each do |unack|
            L.debug "Requeing unacked msg", segment_position: unack.sp
            unack.queue.reject(unack.sp, true)
            unack.queue.basic_get_unacked.reject! { |u| u.channel == self && u.delivery_tag == unack.tag }
          end
          @unacked.clear
        end
        @has_capacity.close
        @next_msg_body_file.try &.close
        @client.vhost.event_tick(EventType::ChannelClosed)
        L.debug "Channel closed"
      end

      protected def next_delivery_tag(queue : Queue, sp, no_ack, consumer) : UInt64
        tag = @delivery_tag.add(1, :relaxed)
        unless no_ack
          @unack_lock.synchronize do
            @unacked.push Unack.new(tag, queue, sp, consumer, RoughTime.monotonic)
          end
          add = consumer ? 0u32 : 1u32
          basic_get_unacked_count = @basic_get_unacked_count.add(add, :relaxed) + add
          @has_capacity.set(false) if 0 < @global_prefetch_count <= (@unacked.size - basic_get_unacked_count)
        end
        tag
      end

      # Iterate over all unacked messages and see if any has been unacked longer than the queue's consumer timeout
      def check_consumer_timeout
        @unack_lock.synchronize do
          queues = Set(Queue).new # only check first delivered message per queue
          @unacked.each do |unack|
            if queues.add? unack.queue
              if timeout = unack.queue.consumer_timeout
                unacked_ms = RoughTime.monotonic - unack.delivered_at
                if unacked_ms > timeout.milliseconds
                  code = ChannelReplyCode::PRECONDITION_FAILED
                  send AMQP::Frame::Channel::Close.new(@id, code.value, "#{code} - consumer timeout", 60_u16, 20_u16)
                  break
                end
              end
            end
          end
        end
      end

      def has_capacity? : Bool
        return true if @global_prefetch_count.zero?
        consumer_unacked = @unacked.size - @basic_get_unacked_count.get(:relaxed)
        consumer_unacked < @global_prefetch_count
      end

      # Notify has capcity if no capcity before the block but after
      private def notify_has_capacity(&)
        @unack_lock.synchronize do
          had_capacity = has_capacity?
          begin
            yield
          ensure
            @has_capacity.set(true) if !had_capacity && has_capacity?
          end
        end
      end

      def cancel_consumer(frame)
        L.debug "Cancelling consumer", consumer_tag: frame.consumer_tag
        if idx = @consumers.index { |cons| cons.tag == frame.consumer_tag }
          c = @consumers.delete_at idx
          c.close
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
            File.tempfile("channel.", nil, dir: @client.vhost.data_dir).tap do |f|
              f.sync = true
              f.read_buffering = false
              f.delete
            end
          end
      end

      def tx_select(frame)
        if @confirm
          @client.send_precondition_failed(frame, "Channel already in confirm mode")
          return
        end
        @tx = true
        send AMQP::Frame::Tx::SelectOk.new(frame.channel)
      end

      def tx_commit(frame)
        return @client.send_precondition_failed(frame, "Not in transaction mode") unless @tx
        process_tx_acks
        process_tx_publishes
        @client.vhost.sync
        send AMQP::Frame::Tx::CommitOk.new(frame.channel)
      end

      private def process_tx_publishes
        next_msg_body_file.rewind
        @tx_publishes.each do |tx_msg|
          tx_msg.message.timestamp = RoughTime.unix_ms
          ok = @client.vhost.publish(tx_msg.message, tx_msg.immediate, @visited, @found_queues)
          basic_return(tx_msg.message, tx_msg.mandatory, tx_msg.immediate) unless ok
          # skip to next msg body in the next_msg_body_file
          tx_msg.message.body_io.seek(tx_msg.message.bodysize, IO::Seek::Current)
        end
        @tx_publishes.clear
      ensure
        next_msg_body_file.rewind
      end

      private def process_tx_acks
        notify_has_capacity do
          @tx_acks.each do |tx_ack|
            if idx = @unacked.bsearch_index { |u, _| u.tag >= tx_ack.delivery_tag }
              raise "BUG: Delivery tag not found" unless @unacked[idx].tag == tx_ack.delivery_tag
              L.debug "Unacked bsearch found tag", delivery_tag: tx_ack.delivery_tag, index: idx
              if tx_ack.multiple
                (idx + 1).times do
                  unack = @unacked.shift
                  @basic_get_unacked_count.sub(1u32, :relaxed) if unack.consumer.nil?
                  if tx_ack.negative
                    do_reject(tx_ack.requeue, unack)
                  else
                    do_ack(unack)
                  end
                end
              else
                unack = @unacked.delete_at(idx)
                @basic_get_unacked_count.sub(1u32, :relaxed) if unack.consumer.nil?
                if tx_ack.negative
                  do_reject(tx_ack.requeue, unack)
                else
                  do_ack(unack)
                end
              end
            end
          end
          @tx_acks.clear
        end
      end

      def tx_rollback(frame)
        return @client.send_precondition_failed(frame, "Not in transaction mode") unless @tx
        @tx_publishes.clear
        @tx_acks.clear
        next_msg_body_file.rewind
        send AMQP::Frame::Tx::RollbackOk.new(frame.channel)
      end

      def flush
        @client.flush
      end

      class ClosedError < Error; end
    end
  end
end
