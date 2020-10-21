require "file_utils"
require "./queue"
require "./schema"

module AvalancheMQ
  class DurableQueue < Queue
    @durable = true
    @acks = 0_u32
    @ack_lock = Mutex.new(:unchecked)
    @enq_lock = Mutex.new(:unchecked)

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
      File.write(File.join(@index_dir, ".queue"), @name)
      @enq = File.open(File.join(@index_dir, "enq"), "a+")
      @enq.buffer_size = Config.instance.file_buffer_size
      SchemaVersion.verify_or_prefix(@enq, :index)
      @ack = File.open(File.join(@index_dir, "ack"), "a+")
      @ack.buffer_size = Config.instance.file_buffer_size
      SchemaVersion.verify_or_prefix(@ack, :index)
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @enq_lock.lock
      @ack_lock.lock
      i = 0
      @enq.close
      @enq = File.new(File.join(@index_dir, "enq.tmp"), "w")
      @enq.buffer_size = Config.instance.file_buffer_size
      SchemaVersion.prefix(@enq, :index)
      @ready.locked_each do |all_ready|
        unacked = @unacked.all_segment_positions.sort!.each
        next_unacked = unacked.next.as?(SegmentPosition)
        while sp = all_ready.next.as?(SegmentPosition)
          while next_unacked && next_unacked < sp
            @enq.write_bytes next_unacked
            i += 1
            next_unacked = unacked.next.as?(SegmentPosition)
          end
          @enq.write_bytes sp
          i += 1
        end
        while next_unacked
          @enq.write_bytes next_unacked
          i += 1
          next_unacked = unacked.next.as?(SegmentPosition)
        end
      end

      @log.info { "Wrote #{i} SPs to new enq file" }
      File.rename File.join(@index_dir, "enq.tmp"), File.join(@index_dir, "enq")
      @enq.fsync(flush_metadata: true)

      @ack.truncate
      SchemaVersion.prefix(@ack, :index)
      @acks = 0_u32
    ensure
      @ack_lock.unlock
      @enq_lock.unlock
    end

    def close : Bool
      super || return false
      @log.debug { "Closing index files" }
      @enq_lock.synchronize do
        @enq.close
      end
      @ack_lock.synchronize do
        @ack.close
      end
      true
    end

    def delete : Bool
      super || return false
      FileUtils.rm_rf @index_dir
      true
    end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      super || return false
      @enq_lock.synchronize do
        @log.debug { "writing #{sp} to enq" }
        @enq.write_bytes sp
        @enq.flush if persistent
      end
      true
    end

    protected def delete_message(sp : SegmentPosition, persistent = false) : Nil
      super
      @ack_lock.synchronize do
        @log.debug { "writing #{sp} to ack" }
        @ack.write_bytes sp
        @ack.flush if persistent
        @acks += 1
      end
      if @acks > @ready.size && @acks >= Config.instance.queue_max_acks
        time = Time.measure do
          compact_index!
        end
        @log.info { "Compacting index took #{time.total_milliseconds} ms" }
      end
    end

    def purge
      @log.info "Purging"
      @enq_lock.synchronize do
        @enq.truncate
        SchemaVersion.prefix(@enq, :index)
        @enq.fsync(flush_metadata: true)
      end
      @ack_lock.synchronize do
        @ack.truncate
        SchemaVersion.prefix(@ack, :index)
        @acks = 0_u32
      end
      super
    end

    def fsync_enq
      return if @closed
      # @log.debug "fsyncing enq"
      @enq.fsync(flush_metadata: false)
    end

    def fsync_ack
      return if @closed
      # @log.debug "fsyncing ack"
      @ack.fsync(flush_metadata: false)
    end

    SP_SIZE = SegmentPosition::BYTESIZE

    private def restore_index : Nil
      @log.info "Restoring index"
      SchemaVersion.migrate(File.join(@index_dir, "enq"), :index)
      SchemaVersion.migrate(File.join(@index_dir, "ack"), :index)
      File.open(File.join(@index_dir, "enq")) do |enq|
        File.open(File.join(@index_dir, "ack")) do |ack|
          enq.buffer_size = Config.instance.file_buffer_size
          ack.buffer_size = Config.instance.file_buffer_size
          enq.advise(File::Advice::Sequential)
          ack.advise(File::Advice::Sequential)
          SchemaVersion.verify(enq, :index)
          SchemaVersion.verify(ack, :index)

          @acks = ack_count = ((ack.size - sizeof(Int32)) // SP_SIZE).to_u32
          acked = Array(SegmentPosition).new(ack_count)
          loop do
            acked << SegmentPosition.from_io ack
          rescue IO::EOFError
            break
          end
          acked.sort!
          # to avoid repetetive allocations in Dequeue#increase_capacity
          # we redeclare the ready queue with a larger initial capacity
          enq_count = (enq.size.to_i64 - ack.size - (sizeof(Int32) * 2)) // SP_SIZE
          capacity = Math.max(enq_count, 1024)
          @ready = ready = ReadyQueue.new Math.pw2ceil(capacity)
          loop do
            sp = SegmentPosition.from_io enq
            next if acked.bsearch { |asp| asp >= sp } == sp
            ready << sp
          rescue IO::EOFError
            break
          end
          @log.info { "#{message_count} messages" }
          message_available if message_count > 0
        end
      end
    rescue ex : IO::Error
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
