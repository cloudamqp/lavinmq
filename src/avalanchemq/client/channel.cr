require "logger"
require "./channel/consumer"
require "../queue"
require "../amqp"
require "../stats"
require "../sortable_json"
require "../error"

module AvalancheMQ
  class Client
    class Channel
      include Stats
      include SortableJSON

      getter id, client, prefetch_size, prefetch_count, global_prefetch,
        confirm, log, consumers, name
      property? running = true
      getter? client_flow

      @next_publish_mandatory = false
      @next_publish_immediate = false
      @next_publish_exchange_name : String?
      @next_publish_routing_key : String?
      @next_msg_size = 0_u64
      @next_msg_body_pos = 0
      @next_msg_props : AMQP::Properties?
      @log : Logger
      @client_flow = true
      @prefetch_size = 0_u32
      @prefetch_count = 0_u16
      @confirm_total = 0_u64
      @confirm = false
      @global_prefetch = false
      @consumers = Array(Consumer).new
      @delivery_tag = 0_u64
      @unacked = Deque(Unack).new
      @unack_lock = Mutex.new(:unchecked)

      rate_stats(%w(ack get publish deliver redeliver reject confirm return_unroutable))
      property deliver_count, redeliver_count

      def initialize(@client : Client, @id : UInt16)
        @log = @client.log.dup
        @log.progname += " channel=#{@id}"
        @name = "#{@client.channel_name_prefix}[#{@id}]"
        tmp_path = File.join(@client.vhost.data_dir, "tmp", Random::Secure.urlsafe_base64)
        @next_msg_body = File.open(tmp_path, "w+")
        @next_msg_body.sync = true
        @next_msg_body.read_buffering = false
        @next_msg_body.delete
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

      MAX_MESSAGE_BODY_SIZE = 512 * 1024 * 1024

      def next_msg_headers(frame, ts)
        raise Error::UnexpectedFrame.new(frame) if @next_publish_exchange_name.nil?
        raise Error::UnexpectedFrame.new(frame) if frame.class_id != 60
        valid_expiration?(frame) || return
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
        finish_publish(@next_msg_body, ts) if frame.body_size.zero?
      end

      def add_content(frame, ts)
        if @next_publish_exchange_name.nil? || @next_msg_props.nil?
          frame.body.skip(frame.body_size)
          raise Error::UnexpectedFrame.new(frame)
        end
        if frame.body_size == @next_msg_size
          finish_publish(frame.body, ts)
        else
          copied = IO.copy(frame.body, @next_msg_body, frame.body_size)
          @next_msg_body_pos += copied
          if copied != frame.body_size
            raise IO::Error.new("Could only copy #{copied} of #{frame.body_size} bytes")
          end
          if @next_msg_body_pos == @next_msg_size
            @next_msg_body.rewind
            begin
              finish_publish(@next_msg_body, ts)
            ensure
              @next_msg_body.truncate
              @next_msg_body.rewind
              @next_msg_body_pos = 0
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

      private def publish_and_return(msg)
        return true if direct_reply?(msg)
        if user_id = msg.properties.user_id
          if user_id != @client.user.name && !@client.user.can_impersonate?
            text = "Message's user_id property '#{user_id}' doesn't match actual user '#{@client.user.name}'"
            @log.error { text }
            raise Error::PreconditionFailed.new(text)
          end
        end
        @confirm_total += 1 if @confirm
        ok = @client.vhost.publish msg, @next_publish_immediate, @visited, @found_queues
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
          if @client.queue_exclusive_to_other_client?(q)
            @client.send_resource_locked(frame, "Exclusive queue")
          elsif q.has_exclusive_consumer?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' in exclusive use")
          elsif q.internal?
            @client.send_access_refused(frame, "Queue '#{frame.queue}' in vhost '#{@client.vhost.name}' is internal")
          else
            if env = q.basic_get(frame.no_ack)
              persistent = env.message.properties.delivery_mode == 2_u8
              delivery_tag = next_delivery_tag(q, env.segment_position,
                persistent, frame.no_ack,
                nil)
              get_ok = AMQP::Frame::Basic::GetOk.new(frame.channel, delivery_tag,
                env.redelivered, env.message.exchange_name,
                env.message.routing_key, q.message_count)
              deliver(get_ok, env.message)
              @get_count += 1
              @redeliver_count += 1 if env.redelivered
            else
              send AMQP::Frame::Basic::GetEmpty.new(frame.channel)
            end
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
            @log.debug { "Unacked bsearch found tag:#{delivery_tag} at index:#{idx}" }
            @unacked.delete_at idx
          end
        end
      end

      private def delete_all_unacked
        @unack_lock.synchronize do
          begin
            @unacked.each { |unack| yield unack }
          ensure
            @unacked.clear
          end
        end
      end

      private def delete_consumers_unacked(consumer)
        @unack_lock.synchronize do
          @unacked.delete_if do |unack|
            unack.consumer == consumer
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
        unack.queue.ack(unack.sp, persistent: unack.persistent)
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
      end

      def basic_qos(frame) : Nil
        @client.send_not_implemented(frame) if frame.prefetch_size != 0
        notify_queues = frame.prefetch_count > @prefetch_count > 0
        notify_queues ||= frame.prefetch_count.zero? && @prefetch_count > 0
        @prefetch_size = frame.prefetch_size
        @prefetch_count = frame.prefetch_count
        @global_prefetch = frame.global
        send AMQP::Frame::Basic::QosOk.new(frame.channel)
        @consumers.each { |c| c.queue.consumer_available } if notify_queues
      end

      def basic_recover(frame) : Nil
        @consumers.each { |c| c.recover(frame.requeue) }
        delete_all_unacked do |unack|
          unack.queue.reject(unack.sp, true) if unack.consumer.nil?
        end
        send AMQP::Frame::Basic::RecoverOk.new(frame.channel)
        if frame.requeue
          @consumers.each do |c|
            q = c.queue
            q.consumer_available
            q.message_available
          end
        end
      end

      def close
        @running = false
        @consumers.each do |c|
          delete_consumers_unacked(c)
          c.queue.rm_consumer(c)
        end
        @consumers.clear
        delete_all_unacked do |unack|
          @log.debug { "Requeing unacked msg #{unack.sp}" }
          unack.queue.reject(unack.sp, true)
        end
        @next_msg_body.close
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
        @unack_lock.synchronize do
          @unacked.delete_if do |unack|
            if unack.consumer == consumer
              yield unack.sp
              true
            end
          end
        end
      end

      def cancel_consumer(frame)
        @log.debug { "Cancelling consumer '#{frame.consumer_tag}'" }
        if idx = @consumers.index { |cons| cons.tag == frame.consumer_tag }
          c = @consumers.delete_at idx
          c.queue.rm_consumer(c, basic_cancel: true)
        end
        unless frame.no_wait
          send AMQP::Frame::Basic::CancelOk.new(frame.channel, frame.consumer_tag)
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
