require "./durable_queue"

module LavinMQ::AMQP
  class PriorityQueue < Queue
    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      self.validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    def self.validate_arguments!(arguments)
      int_zero_255 = ArgumentValidator::IntValidator.new(min_value: 0, max_value: 255)
      if value = arguments["x-max-priority"]?
        int_zero_255.validate!("x-max-priority", value)
      else
        # should never end up here
        raise LavinMQ::Error::PreconditionFailed.new("x-max-priority argument is required for priority queues")
      end
      super
    end

    private def handle_arguments
      super
      @effective_args << "x-max-priority"
    end

    private def init_msg_store(msg_dir)
      replicator = durable? ? @vhost.@replicator : nil
      max_priority = @arguments["x-max-priority"]?.try(&.as?(Int)) || 0u8
      PriorityMessageStore.new(max_priority.to_u8, msg_dir, replicator, metadata: @metadata)
    end

    class PriorityMessageStore < MessageStore
      # These are just to make the compiler happy. They are never used.
      @acks = uninitialized Hash(UInt32, MFile)
      @wfile = uninitialized MFile
      @rfile = uninitialized MFile
      @wfile_id = 0
      @rfile_id = 0

      protected def initialize(
        @max_priority : UInt8,
        @msg_dir : String,
        @replicator : Clustering::Replicator?,
        @durable : Bool = true,
        @metadata : ::Log::Metadata = ::Log::Metadata.empty,
      )
        @log = Logger.new(Log, metadata.extend({max_prio: @max_priority.to_s}))
        @stores = Array(MessageStore).new(1 + @max_priority)

        init_sub_stores(@stores)
        migrate_from_single_store

        @empty = BoolChannel.new(empty?)
      end

      private def init_sub_stores(stores)
        0.upto(@max_priority) do |i|
          sub_msg_dir = File.join(@msg_dir, "prio.#{i.to_s.rjust(3, '0')}")
          Dir.mkdir_p sub_msg_dir
          store = MessageStore.new(sub_msg_dir, @replicator, @durable, metadata: @metadata.extend({prio: i.to_s}))
          stores << store
        end
      end

      private def migrate_from_single_store
        return unless needs_migrate?
        unless empty?
          raise "Message store #{@msg_dir} contains messages that should be migrated, " \
                "but substores are not empty. Migration aborted, manually intervention needed."
        end
        old_store = MessageStore.new(@msg_dir, @replicator, @durable, metadata: @metadata)
        msg_count = old_store.size
        @log.info { "Migrating #{msg_count} message" }
        i = 0u32
        while env = old_store.shift?
          push env.message
          Fiber.yield if ((i &+= 1) % 8096).zero?
        end
        if size != msg_count
          raise "Message count mismatch when migration message store #{@msg_dir}. #{msg_count} messages before migration, #{size} after."
        end
        @log.info { "Migration complete" }
        old_store.close
        i = 0u32
        delete_wg = WaitGroup.new
        pattern = %r{^(msgs|acks|meta)\.}
        Dir.each_child(@msg_dir) do |f|
          if f.matches? pattern
            filepath = File.join(@msg_dir, f)
            File.delete? filepath
            @replicator.try &.delete_file(filepath, delete_wg)
            Fiber.yield if ((i &+= 1) % 8096).zero?
          end
        end
        delete_wg.wait
      end

      private def needs_migrate?
        Dir.each_child(@msg_dir) do |f|
          return true if f.starts_with? "msgs."
        end
        false
      end

      # returns the substore for the priority
      private def store_for(prio : UInt8, &)
        unless 0 <= prio <= @max_priority
          raise ArgumentError.new "Priority must be between 0 and #{@max_priority}, got #{prio}"
        end
        yield @stores[prio]
      end

      private def store_for(sp : SegmentPosition, &)
        store_for(sp.priority) do |store|
          yield store
        end
      end

      private def store_for(msg, &)
        store_for(msg.properties.priority || 0u8) do |store|
          yield store
        end
      end

      def size
        @stores.sum(&.size)
      end

      def bytesize
        @stores.sum(&.bytesize)
      end

      def empty? : Bool
        size.zero?
      end

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        prio = Math.min(msg.properties.priority || 0u8, @max_priority)
        was_empty = size.zero?
        @empty.set false if was_empty
        sp = store_for prio, &.push(msg)
        sp
      end

      def requeue(sp : SegmentPosition)
        raise ClosedError.new if @closed
        was_empty = size.zero?
        @empty.set false if was_empty
        store_for sp, &.requeue(sp)
      end

      def first? : Envelope?
        raise ClosedError.new if @closed
        @stores.reverse_each do |s|
          envelope = s.first?
          return envelope unless envelope.nil?
        end
      end

      def shift?(consumer = nil) : Envelope?
        raise ClosedError.new if @closed
        @stores.reverse_each do |s|
          envelope = s.shift?(consumer)
          return envelope unless envelope.nil?
        end
      end

      def [](sp : SegmentPosition) : BytesMessage
        raise ClosedError.new if @closed
        store_for sp, &.[sp]
      end

      def delete(sp) : Nil
        raise ClosedError.new if @closed
        store_for sp, &.delete(sp)
      end

      def purge(max_count : Int = UInt32::MAX) : UInt32
        raise ClosedError.new if @closed
        count = 0u32
        while count < max_count && (env = shift?)
          delete(env.segment_position)
          count += 1
          break if count >= max_count
          Fiber.yield if (count % PURGE_YIELD_INTERVAL).zero?
        end
        count
      end

      def purge_all
        @stores.each &.purge_all
      end

      def delete
        @closed = true
        @empty.close
        @stores.each &.delete
        FileUtils.rm_rf @msg_dir
      end

      def close : Nil
        return if @closed
        @closed = true
        @stores.each &.close
        @empty.close
      end
    end
  end

  class DurablePriorityQueue < PriorityQueue
    def self.create(vhost : VHost, name : String,
                    exclusive : Bool = false, auto_delete : Bool = false,
                    arguments : AMQP::Table = AMQP::Table.new)
      self.validate_arguments!(arguments)
      new vhost, name, exclusive, auto_delete, arguments
    end

    def durable?
      true
    end
  end
end
