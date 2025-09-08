require "./durable_queue"

module LavinMQ::AMQP
  class PriorityQueue < Queue
    private def handle_arguments
      super
      @effective_args << "x-max-priority"
    end

    private def init_msg_store(msg_dir)
      replicator = durable? ? @vhost.@replicator : nil
      max_priority = @arguments["x-max-priority"]?.try(&.as?(Int)) || 0u8
      PriorityMessageStore.new(max_priority.to_u8, msg_dir, replicator, metadata: L.context)
    end

    class PriorityMessageStore < MessageStore
      include LavinMQ::Logging::Loggable
      # These are just to make the compiler happy. They are never used.
      @acks = uninitialized Hash(UInt32, MFile)
      @wfile = uninitialized MFile
      @rfile = uninitialized MFile
      @wfile_id = 0
      @rfile_id = 0

      def initialize(
        @max_priority : UInt8,
        @msg_dir : String,
        @replicator : Clustering::Replicator?,
        @durable : Bool = true,
        metadata : ::Log::Metadata = ::Log::Metadata.empty,
      )
        L.context(metadata.extend({max_prio: @max_priority}))
        @stores = Array(MessageStore).new(1 + @max_priority)

        init_sub_stores(@stores)
        migrate_from_single_store

        @empty.set empty?
      end

      private def init_sub_stores(stores)
        0.upto(@max_priority) do |i|
          sub_msg_dir = File.join(@msg_dir, "prio.#{i.to_s.rjust(3, '0')}")
          Dir.mkdir_p sub_msg_dir
          store = MessageStore.new(sub_msg_dir, @replicator, @durable, metadata: L.context.extend({prio: i}))
          @size += store.size
          @bytesize += store.bytesize
          stores << store
        end
      end

      private def migrate_from_single_store
        return unless needs_migrate?
        unless empty?
          raise "Message store #{@msg_dir} contains messages that should be migrated, " \
                "but substores are not empty. Migration aborted, manually intervention needed."
        end
        old_store = MessageStore.new(@msg_dir, @replicator, @durable, metadata: L.context)
        msg_count = old_store.size
        L.info "Migrating messages", count: msg_count
        i = 0u32
        while env = old_store.shift?
          msg = env.message
          push LavinMQ::Message.new(
            msg.timestamp,
            msg.exchange_name,
            msg.routing_key,
            msg.properties,
            msg.bodysize,
            IO::Memory.new(msg.body)
          )
          Fiber.yield if (i &+= 8096).zero?
        end
        if size != msg_count
          raise "Message count mismatch when migration message store #{@msg_dir}. #{msg_count} messages before migration, #{size} after."
        end
        L.info "Migration complete"
        old_store.close
        i = 0u32
        delete_wg = WaitGroup.new
        Dir.each_child(@msg_dir) do |f|
          if f.starts_with?("msgs.") || f.starts_with?("acks.")
            filepath = File.join(@msg_dir, f)
            File.delete? filepath
            @replicator.try &.delete_file(filepath, delete_wg)
            Fiber.yield if (i &+= 8096).zero?
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
        # Since @stores is sorted with highets prio first, we do lookup from
        # end of the array. We must add one step because last item is -1 not -0.
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

      def push(msg) : SegmentPosition
        raise ClosedError.new if @closed
        prio = Math.min(msg.properties.priority || 0u8, @max_priority)
        sp = store_for prio, &.push(msg)
        was_empty = @size.zero?
        @bytesize += sp.bytesize
        @size += 1
        @empty.set false if was_empty
        sp
      end

      def requeue(sp : SegmentPosition)
        raise ClosedError.new if @closed
        store_for sp, &.requeue(sp)
        was_empty = @size.zero?
        @bytesize += sp.bytesize
        @size += 1
        @empty.set false if was_empty
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
    def durable?
      true
    end
  end
end
