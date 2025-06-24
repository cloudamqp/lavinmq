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
      PriorityMessageStore.new(max_priority.to_u8, msg_dir, replicator, metadata: @metadata)
    end

    class PriorityMessageStore < MessageStore
      @stores : Array(MessageStore)

      def initialize(@max_priority : UInt8, @msg_dir : String, @replicator : Clustering::Replicator?, @durable : Bool = true, @metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @stores = Array(MessageStore).new(1 + @max_priority)
        @log = Log.for(self, @metadata)

        (0..@max_priority).reverse_each do |i|
          sub_msg_dir = File.join(@msg_dir, i.to_s)
          Dir.mkdir_p sub_msg_dir
          store = MessageStore.new(sub_msg_dir, @replicator, @durable, metadata: @metadata)
          @size += store.size
          @bytesize += store.bytesize
          @stores << store
        end
        @empty.set empty?
      end

      # returns the substore for the priority
      private def store_for(prio : UInt8, &)
        yield @stores[@stores.size - prio - 1]
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
        sp = store_for msg, &.push(msg)
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
        @stores.each do |s|
          envelope = s.first?
          return envelope unless envelope.nil?
        end
      end

      def shift?(consumer = nil) : Envelope?
        raise ClosedError.new if @closed
        @stores.each do |s|
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
