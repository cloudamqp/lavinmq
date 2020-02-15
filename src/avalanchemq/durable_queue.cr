require "file_utils"
require "./queue"

module AvalancheMQ
  class DurableQueue < Queue
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      Dir.mkdir_p @index_dir
      @enq = File.open(File.join(@index_dir, "enq"), "a+")
      @enq.buffer_size = Config.instance.file_buffer_size
      @enq.sync = true
      @ack = File.open(File.join(@index_dir, "ack"), "a+")
      @ack.buffer_size = Config.instance.file_buffer_size
      @ack.sync = true
      @acks = 0_u32
      @ack_lock = Mutex.new
      @enq_lock = Mutex.new
      restore_index
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @enq_lock.synchronize do
        @ack_lock.synchronize do
          # read all acked SPs into a Set
          @ack.rewind
          acked = Set(SegmentPosition).new(@ack.size // sizeof(SegmentPosition))
          loop do
            acked << SegmentPosition.from_io @ack
          rescue IO::EOFError
            break
          end
          @log.debug { "Read #{acked.size} acked SPs" }

          # Read all enqueued SPs and write to a new enq file
          # unless the SP is already acked
          @enq.rewind
          i = 0_u32
          File.open(File.join(@index_dir, "enq.tmp"), "w") do |f|
            loop do
              sp = SegmentPosition.from_io @enq
              next if acked.includes? sp
              f.write_bytes sp
              i += 1
            rescue IO::EOFError
              break
            end
          end
          @log.debug { "Wrote #{i} SPs to new enq file" }
          @enq.close
          File.rename File.join(@index_dir, "enq.tmp"), File.join(@index_dir, "enq")
          @enq = File.open(File.join(@index_dir, "enq"), "a+")
          @enq.sync = true
          @enq.buffer_size = Config.instance.file_buffer_size
          @enq.fsync(flush_metadata: true)

          @ack.truncate
          @ack.fsync(flush_metadata: false)
          @acks = 0_u32
        end
      end
    end

    def close : Bool
      super.tap do |closed|
        next unless closed
        compact_index! unless @deleted
        @log.debug { "Closing index files" }
        @ack.close
        @enq.close
      end
    end

    def delete
      FileUtils.rm_rf @index_dir if super
    end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      super || return false
      if persistent
        @enq_lock.synchronize do
          @enq.write_bytes sp
        end
      end
      true
    end

    def get(no_ack : Bool, &blk : Envelope? -> Nil)
      super(no_ack) do |env|
        if env && no_ack
          persistent = env.message.properties.delivery_mode == 2_u8
          if persistent
            @ack_lock.synchronize do
              @ack.write_bytes env.segment_position
              @acks += 1
            end
            compact_index! if @acks > Config.instance.queue_max_acks
          end
        end
        yield env
      end
    end

    def ack(sp : SegmentPosition, persistent : Bool) : Nil
      super
      if persistent
        @ack_lock.synchronize do
          @ack.write_bytes sp
          @acks += 1
        end
        compact_index! if @acks > Config.instance.queue_max_acks
      end
    end

    private def drop(sp, delete_in_ready, persistent) : Nil
      super
      if persistent
        @ack_lock.synchronize do
          @ack.write_bytes sp
          @acks += 1
        end
        compact_index! if @acks > Config.instance.queue_max_acks
      end
    end

    def purge
      @log.info "Purging"
      @enq_lock.synchronize do
        @enq.truncate
      end
      @ack_lock.synchronize do
        @ack.truncate
        @acks = 0_u32
      end
      super
    end

    def fsync_enq
      return if @closed
      @log.debug "fsyncing enq"
      @enq.fsync(flush_metadata: false)
    end

    def fsync_ack
      return if @closed
      @log.debug "fsyncing ack"
      @ack.fsync(flush_metadata: false)
    end

    private def restore_index : Nil
      @log.info "Restoring index"
      @ack.rewind
      sp_size = sizeof(SegmentPosition)
      acked = Set(SegmentPosition).new(@ack.size // sp_size)
      loop do
        acked << SegmentPosition.from_io @ack
        @acks += 1
      rescue IO::EOFError
        break
      end
      # to avoid repetetive allocations in Dequeue#increase_capacity
      # we redeclare the ready queue with a larger initial capacity
      capacity = Math.max(@enq.size.to_i64 - @ack.size, sp_size) // sp_size
      @ready = Deque(SegmentPosition).new(capacity)
      @enq.rewind
      loop do
        sp = SegmentPosition.from_io @enq
        next if acked.includes? sp
        @ready << sp
        @segment_ref_count.inc(sp.segment)
      rescue IO::EOFError
        break
      end
      @log.info { "#{message_count} messages" }
      message_available if message_count > 0
    rescue ex : Errno
      @log.error { "Could not restore index: #{ex.inspect}" }
    end

    def enq_file_size
      @enq.size
    end

    def ack_file_size
      @ack.size
    end
  end
end
