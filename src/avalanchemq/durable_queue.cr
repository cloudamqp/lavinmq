require "file_utils"
require "./queue"

module AvalancheMQ
  class DurableQueue < Queue
    @durable = true
    @acks = 0_u32

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      if Dir.exists?(@index_dir)
        restore_index
      else
        Dir.mkdir @index_dir
      end
      @enq = File.open(File.join(@index_dir, "enq"), "a")
      @ack = File.open(File.join(@index_dir, "ack"), "a")
      @ack_lock = Mutex.new
      @enq_lock = Mutex.new
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @ready_lock.lock
      @enq_lock.lock
      @ack_lock.lock
      @enq.close
      i = 0
      File.open(File.join(@index_dir, "enq.tmp"), "w") do |f|
        unacked = @unack_lock.synchronize { @unacked.map &.sp }.sort!.each
        next_unacked = unacked.next
        @ready.each do |sp|
          loop do
            break if next_unacked == Iterator::Stop::INSTANCE
            break if sp < next_unacked.as(SegmentPosition)
            f.write_bytes next_unacked.as(SegmentPosition)
            next_unacked = unacked.next
            i += 1
          end
          f.write_bytes sp
          i += 1
        end
        until next_unacked == Iterator::Stop::INSTANCE
          f.write_bytes next_unacked.as(SegmentPosition)
          next_unacked = unacked.next
          i += 1
        end
      end

      @log.info { "Wrote #{i} SPs to new enq file" }
      File.rename File.join(@index_dir, "enq.tmp"), File.join(@index_dir, "enq")
      @enq = File.open(File.join(@index_dir, "enq"), "a")
      @enq.fsync(flush_metadata: true)

      @ack.truncate
      @acks = 0_u32
    ensure
      @ack_lock.unlock
      @enq_lock.unlock
      @ready_lock.unlock
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
        @enq_lock.synchronize do
          @enq.write_bytes sp
          @enq.flush if persistent
        end
      true
    end

    def get(no_ack : Bool, &blk : Envelope? -> Nil)
      super(no_ack) do |env|
        if env && no_ack
          @ack_lock.synchronize do
            @ack.write_bytes env.segment_position
            @ack.flush if env.message.persistent?
            @acks += 1
          end
          compact_index! if @acks >= Config.instance.queue_max_acks
        end
        yield env
      end
    end

    def ack(sp : SegmentPosition, persistent : Bool) : Nil
      super
      @ack_lock.synchronize do
        @ack.write_bytes sp
        @ack.flush if persistent
        @acks += 1
      end
      compact_index! if @acks >= Config.instance.queue_max_acks
    end

    private def drop(sp, delete_in_ready, persistent) : Nil
      super
      @ack_lock.synchronize do
        @ack.write_bytes sp
        @ack.flush if persistent
        @acks += 1
      end
      compact_index! if @acks >= Config.instance.queue_max_acks
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
      sp_size = sizeof(SegmentPosition)

      File.open(File.join(@index_dir, "enq")) do |enq|
        File.open(File.join(@index_dir, "ack")) do |ack|
          enq.buffer_size = Config.instance.file_buffer_size
          ack.buffer_size = Config.instance.file_buffer_size

          acked = Array(SegmentPosition).new(ack.size // sp_size)
          loop do
            acked << SegmentPosition.from_io ack
            @acks += 1
          rescue IO::EOFError
            break
          end
          # to avoid repetetive allocations in Dequeue#increase_capacity
          # we redeclare the ready queue with a larger initial capacity
          capacity = Math.max(enq.size.to_i64 - ack.size, 1024 * sp_size) // sp_size
          @ready = Deque(SegmentPosition).new(capacity)
          loop do
            sp = SegmentPosition.from_io enq
            next if acked.includes? sp
            @ready << sp
          rescue IO::EOFError
            break
          end
          @log.info { "#{message_count} messages" }
          message_available if message_count > 0
        end
      end
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
