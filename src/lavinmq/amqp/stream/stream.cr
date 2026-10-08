require "../queue/durable_queue"
require "./stream_consumer"
require "./stream_cursor_consumer"
require "./stream_message_store"

module LavinMQ::AMQP
  class Stream < DurableQueue
    # Arguments that have no meaning for streams. Rejected at queue declare
    # AND when matched by a policy — otherwise a policy can quietly install
    # state (e.g. `delivery-limit` spawning the inherited drop_redelivered
    # fiber, which dereferences the legacy `@rfile` that streams don't
    # maintain) and crash the server.
    INVALID_ARGUMENTS = {
      "x-dead-letter-exchange",
      "x-dead-letter-routing-key",
      "x-expires",
      "x-delivery-limit",
      "x-overflow",
      "x-single-active-consumer",
      "x-max-priority",
    }

    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      # Validate non-arguments first
      raise LavinMQ::Error::PreconditionFailed.new("A stream cannot be exclusive") if exclusive
      raise LavinMQ::Error::PreconditionFailed.new("A stream cannot be auto-delete") if auto_delete

      validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    def self.validate_arguments!(arguments)
      arguments.each do |key, value|
        if INVALID_ARGUMENTS.includes?(key)
          raise LavinMQ::Error::PreconditionFailed.new("Argument #{key} not allowed for streams")
        end
        if key == "x-max-age"
          ArgumentValidator::MaxAgeValidator.new.validate!(key, value)
        end
      end

      super
    end

    protected def initialize(@vhost : VHost, @name : String,
                             @exclusive = false, @auto_delete = false,
                             @arguments = AMQP::Table.new)
      super
    end

    private def apply_policy_argument(key : String, value : JSON::Any) : Bool
      # Reject policy keys that are forbidden as queue arguments, so a
      # matching policy can't install state the stream doesn't honor.
      # Policy keys are `arguments` keys without the `x-` prefix.
      if INVALID_ARGUMENTS.includes?("x-#{key}")
        @log.debug { "Policy argument #{key} not applicable to streams; skipping" }
        return false
      end

      case key
      when "max-age"
        if max_age_policy = parse_max_age(value.as_s?)
          if current_max = stream_msg_store.max_age
            return false unless current_max > max_age_policy
          end
          @msg_store_lock.synchronize do
            stream_msg_store.max_age = max_age_policy
            @effective_args.delete("x-max-age")
            stream_msg_store.drop_overflow
            ensure_max_age_loop
          end
          return true
        end
        false
      when "max-length"
        unless @max_length.try &.< value.as_i64
          @max_length = value.as_i64
          @msg_store_lock.synchronize do
            stream_msg_store.max_length = @max_length
            @effective_args.delete("x-max-length")
            stream_msg_store.drop_overflow
          end
          return true
        end
        false
      when "max-length-bytes"
        unless @max_length_bytes.try &.< value.as_i64
          @max_length_bytes = value.as_i64
          @msg_store_lock.synchronize do
            stream_msg_store.max_length_bytes = @max_length_bytes
            @effective_args.delete("x-max-length-bytes")
            stream_msg_store.drop_overflow
          end
          return true
        end
        false
      else
        super(key, value)
      end
    end

    delegate last_offset, new_messages, to: @msg_store.as(StreamMessageStore)

    def cursor(start : StreamOffset::Any, filter : ConsumerFilter? = nil) : StreamCursor
      @msg_store_lock.synchronize { stream_msg_store.cursor(start, filter) }
    end

    def requeue(cursor : StreamCursor, sp : SegmentPosition) : Nil
      @msg_store_lock.synchronize { cursor.requeue(sp) }
    end

    # The offset stored for `consumer_tag` by automatic offset tracking
    def stored_offset(consumer_tag : String) : Int64?
      @msg_store_lock.synchronize { stream_msg_store.last_offset_by_consumer_tag(consumer_tag) }
    end

    private def message_expire_loop
      # Streams doesn't handle message expiration
    end

    private def queue_expire_loop
      # Streams doesn't handle queue expiration
    end

    # Streams never expire individual messages (message_expire_loop is a no-op),
    # so the expire fiber must never start. Skipping the check also avoids the
    # inherited MessageStore#first?, which dereferences the legacy @rfile that
    # streams don't maintain and crashes once retention has closed that segment.
    private def should_start_expire_fiber? : Bool
      false
    end

    private def start : Bool
      if @msg_store.closed
        !close
      else
        handle_arguments
        true
      end
    end

    private def init_msg_store(data_dir)
      replicator = @vhost.replicator
      @msg_store = StreamMessageStore.new(data_dir, replicator, metadata: @metadata, persister: @vhost.persister)
    end

    def stream_msg_store : StreamMessageStore
      @msg_store.as(StreamMessageStore)
    end

    def publish(msg : Message) : PublishResult
      publish_internal(msg, nil)
    end

    # save message id / segment position
    protected def publish_internal(msg : Message, dlx_tasks : Argument::DeadLettering::Tasks?) : PublishResult
      return PublishResult::Dropped if @state.closed?
      @msg_store_lock.synchronize do
        @msg_store.push(msg)
        @publish_count.add(1, :relaxed)
      end
      # Notify all waiting stream consumers about new messages
      notify_all_stream_consumers
      PublishResult::Ok
    rescue MessageStore::ClosedError
      # Closed/deleted concurrently after the @state.closed? check; treat as
      # dropped instead of surfacing the race as an error (see Queue#publish_internal).
      # push is the only call here that can raise it, so nothing was stored.
      PublishResult::Dropped
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    # Streams does not support basic_get, so always returns `false`
    def basic_get(no_ack, force = false, & : Envelope -> Nil) : Bool
      false
    end

    # Yields messages from `start` until the end of the stream
    def each_from(start : StreamOffset::Any, & : Envelope -> _) : Nil
      cursor = self.cursor(start)
      while stream_msg_store.shift_with_lease?(@msg_store_lock, cursor) { |env| yield env }
        @deliver_get_count.add(1, :relaxed)
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    ensure
      @msg_store_lock.synchronize { cursor.close } if cursor
    end

    def consume_get(cursor : AMQP::StreamCursor, & : Envelope -> Nil) : Bool
      get(cursor) do |env|
        yield env
        if env.redelivered
          @redeliver_count.add(1, :relaxed)
        else
          @deliver_count.add(1, :relaxed)
          @deliver_get_count.add(1, :relaxed)
        end
      end
    end

    def store_consumer_offset(consumer_tag : String, offset : Int64) : Nil
      @msg_store_lock.synchronize do
        stream_msg_store.store_consumer_offset(consumer_tag, offset)
      end
    end

    # yield the next message in the ready queue
    # returns true if a message was deliviered, false otherwise
    # if we encouncer an unrecoverable ReadError, close queue
    private def get(cursor : AMQP::StreamCursor, & : Envelope -> Nil) : Bool
      raise ClosedError.new if @closed
      # Retention can drop the segment while the delivery is suspended in a
      # socket write
      stream_msg_store.shift_with_lease?(@msg_store_lock, cursor) do |env|
        yield env # deliver the message
      end
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ClosedError.new(cause: ex)
    end

    def ack(sp : SegmentPosition) : Nil
    end

    def reject(sp : SegmentPosition, requeue : Bool)
    end

    private def drop_overflow : Nil
      # Overflow handling is done in StreamMessageStore
    end

    private def notify_all_stream_consumers
      @consumers.each do |consumer|
        if stream_consumer = consumer.as?(AMQP::StreamCursorConsumer)
          stream_consumer.notify_new_message if stream_consumer.waiting_for_messages?
        end
      end
    end

    private def handle_arguments
      super
      @effective_args << "x-queue-type"
      # drop_overflow mutates the store, so take @msg_store_lock like other
      # store access; it can run concurrently with publishes/consumes.
      @msg_store_lock.synchronize do
        max_age = parse_max_age(@arguments["x-max-age"]?)
        stream_msg_store.max_age = max_age
        @effective_args << "x-max-age" if max_age
        # Propagate limits set by super to stream_msg_store
        stream_msg_store.max_length = @max_length
        stream_msg_store.max_length_bytes = @max_length_bytes
        stream_msg_store.drop_overflow
        ensure_max_age_loop
      end
    end

    @max_age_loop_running = false

    # Must be called with @msg_store_lock held
    private def ensure_max_age_loop : Nil
      if @max_age_loop_running
        stream_msg_store.expiry_changed.try_send?(nil)
      elsif stream_msg_store.max_age
        @max_age_loop_running = true
        spawn max_age_loop, name: "Stream#max_age_loop"
      end
    end

    # Sleeps until the oldest segment expires, so it only wakes when there's
    # something to drop. Exits when the stream closes or max-age is removed.
    private def max_age_loop
      loop do
        expiry = @msg_store_lock.synchronize do
          store = stream_msg_store
          if closed? || store.closed || store.max_age.nil?
            @max_age_loop_running = false
            return
          end
          store.drop_expired
          store.next_expiry
        end
        expiry_changed = stream_msg_store.expiry_changed
        if expiry
          select
          when expiry_changed.receive?
          when timeout(Math.max(expiry - RoughTime.utc, 100.milliseconds))
          end
        else
          expiry_changed.receive?
        end
      end
    rescue ex
      @log.error(ex) { "max-age loop failed" }
      @msg_store_lock.synchronize { @max_age_loop_running = false }
    end

    private def parse_max_age(value) : (Time::Span | Time::MonthSpan)?
      return if value.nil?
      if str = value.as?(String)
        if match = str.match(/\A(\d+)([YMDhms])\z/)
          int = match[1].to_i64
          case match[2]
          when "s" then Time::Span.new(seconds: int)
          when "m" then Time::Span.new(minutes: int)
          when "h" then Time::Span.new(hours: int)
          when "D" then Time::Span.new(days: int)
          when "M" then Time::MonthSpan.new(int)
          when "Y" then Time::MonthSpan.new(int * 12)
          else          raise LavinMQ::Error::PreconditionFailed.new("max-age unit unit")
          end
        else
          raise LavinMQ::Error::PreconditionFailed.new("max-age format invalid")
        end
      else
        raise LavinMQ::Error::PreconditionFailed.new("max-age must be a string")
      end
    end

    def purge(max_count : Int = UInt32::MAX) : UInt32
      delete_count = @msg_store_lock.synchronize { @msg_store.purge(max_count) }
      @log.info { "Purged #{delete_count} messages" }
      delete_count
    rescue ex : MessageStore::Error
      @log.error(ex) { "Queue closed due to error" }
      close
      raise ex
    end

    def rm_consumer(consumer : Client::Channel::Consumer)
      super
      if stream_consumer = consumer.as?(AMQP::StreamCursorConsumer)
        @msg_store_lock.synchronize { stream_consumer.cursor.close }
      end
    end
  end
end
