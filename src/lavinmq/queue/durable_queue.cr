require "file_utils"
require "./queue"
require "../schema"

module LavinMQ
  class DurableQueue < Queue
    @durable = true
    @ack_lock = Mutex.new(:checked)
    @enq_lock = Mutex.new(:checked)
    @ack : MFile

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      index_corrupted = false # might need to compact index if index files are corrupted
      if Dir.exists?(@index_dir)
        index_corrupted = restore_index
      else
        Dir.mkdir @index_dir
        File.open(File.join(@index_dir, "enq"), "w") { |f| SchemaVersion.prefix(f, :index) }
        File.open(File.join(@index_dir, "ack"), "w") { |f| SchemaVersion.prefix(f, :index) }
      end
      File.write(File.join(@index_dir, ".queue"), @name)
      @enq = MFile.new(File.join(@index_dir, "enq"), ack_max_file_size)
      @enq.advise(MFile::Advice::DontNeed)
      @enq.seek 0, IO::Seek::End
      @ack = MFile.new(File.join(@index_dir, "ack"), ack_max_file_size)
      @ack.advise(MFile::Advice::DontNeed)
      @ack.seek 0, IO::Seek::End
      compact_index! if index_corrupted
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @enq_lock.lock
      i = 0
      @enq.close
      @enq = MFile.new(File.join(@index_dir, "enq.tmp"), ack_max_file_size + SP_SIZE * (@ready.size + @unacked_count))
      SchemaVersion.prefix(@enq, :index)
      @ready.locked_each do |all_ready|
        unacked = @consumers.flat_map(&.channel.unacked_for_queue(self)).sort!.each
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
      @enq.move(File.join(@index_dir, "enq"))
      @enq.advise(MFile::Advice::DontNeed)

      @ack.delete
      @ack.close
      @ack = MFile.new(File.join(@index_dir, "ack"), Math.max(ack_max_file_size, @enq.size // 2))
      SchemaVersion.prefix(@ack, :index)
      @ack.advise(MFile::Advice::DontNeed)
    ensure
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
      @log.debug { "Deleting index directory" }
      FileUtils.rm_rf @index_dir
      true
    end

    def publish(sp : SegmentPosition, persistent = false) : Bool
      super || return false
      @enq_lock.synchronize do
        @log.debug { "writing #{sp} to enq" }
        begin
          @enq.write_bytes sp
        rescue IO::EOFError
          @log.debug { "Out of capacity in enq file, resizeing" }
          @enq.resize(@enq.size + ack_max_file_size)
          @enq.write_bytes sp
        end
      end
      true
    end

    protected def delete_message(sp : SegmentPosition) : Nil
      super
      @ack_lock.synchronize do
        begin
          @log.debug { "writing #{sp} to ack" }
          @ack.write_bytes sp
        rescue IO::EOFError
          half_enq_size = @enq.size // 2
          if @ack.size < half_enq_size && @ack.size < 512 * 1024**2
            @ack.resize half_enq_size + SP_SIZE
            @log.info { "Expanded ack file to avoid index compactation for long queue" }
            @ack.write_bytes sp
          else
            time = Time.measure do
              compact_index!
            end
            @log.info { "Compacting index took #{time.total_milliseconds} ms" }
          end
        end
      end
    end

    def purge(max_count : Int? = nil) : UInt32
      delete_count = super(max_count)
      return 0_u32 if delete_count.zero?
      if max_count.nil?
        @enq_lock.synchronize do
          @enq.close(truncate_to_size: false)
          @enq = MFile.new(File.join(@index_dir, "enq.tmp"), ack_max_file_size)
          SchemaVersion.prefix(@enq, :index)
          @enq.move(File.join(@index_dir, "enq"))
          @enq.advise(MFile::Advice::DontNeed)
        end
        @ack_lock.synchronize do
          @ack.close(truncate_to_size: false)
          @ack = MFile.new(File.join(@index_dir, "ack.tmp"), ack_max_file_size)
          SchemaVersion.prefix(@ack, :index)
          @ack.move(File.join(@index_dir, "ack"))
          @ack.advise(MFile::Advice::DontNeed)
        end
      else
        compact_index!
      end
      delete_count
    end

    def fsync_enq
      return if @closed
      # @log.debug "fsyncing enq"
      @enq.fsync
    end

    def fsync_ack
      return if @closed
      # @log.debug "fsyncing ack"
      @ack.fsync
    end

    SP_SIZE = SegmentPosition::BYTESIZE.to_i64

    # Returns weather queue index should be compacted or not
    private def restore_index : Bool
      @log.info { "Restoring index" }
      acked = restore_acks
      restore_enq(acked)
    rescue ex : IO::Error
      @log.error { "Could not restore index: #{ex.inspect}" }
      false
    end

    private def restore_enq(acked) : Bool
      enq_path = File.join(@index_dir, "enq")
      SchemaVersion.migrate(enq_path, :index)
      File.open(enq_path, "r+") do |enq|
        enq.buffer_size = Config.instance.file_buffer_size
        enq.advise(File::Advice::Sequential)
        SchemaVersion.verify(enq, :index)
        truncate_sparse_file(enq)

        # To avoid repetetive allocations in Dequeue#increase_capacity
        # we redeclare the ready queue with a larger initial capacity
        enq_count = (enq.size.to_i64 - (sizeof(Int32))) // SP_SIZE - acked.size
        capacity = Math.max(enq_count, 1024)
        @ready = ready = @ready.class.new Math.pw2ceil(capacity)

        lost_msgs = 0
        loop do
          sp = SegmentPosition.from_io enq
          if sp.zero?
            # can be holes in the file, because lseek HOLE rounds up to nearest block
            goto_next_block(enq)
            next
          end
          next if acked.bsearch { |asp| asp >= sp } == sp
          # SP refers to a segment that has already been deleted
          unless @vhost.@segments.has_key? sp.segment
            lost_msgs += 1
            next
          end
          ready << sp
          @vhost.increase_segment_references(sp.segment)
        rescue IO::EOFError
          break
        end
        @log.info { "#{message_count} messages" }
        if lost_msgs > 0
          @log.error { "#{lost_msgs} dropped due to reference to missing segment file" }
          return true # should compact_index! (but can't do it here)
        end
      end
      false
    end

    private def restore_acks
      ack_path = File.join(@index_dir, "ack")
      SchemaVersion.migrate(ack_path, :index)
      File.open(ack_path, "r+") do |ack|
        ack.buffer_size = Config.instance.file_buffer_size
        ack.advise(File::Advice::Sequential)
        SchemaVersion.verify(ack, :index)
        truncate_sparse_file(ack)

        ack_count = ((ack.size - sizeof(Int32)) // SP_SIZE).to_u32
        acked = Array(SegmentPosition).new(ack_count)

        loop do
          sp = SegmentPosition.from_io ack
          if sp.zero?
            # can be holes in the file, because lseek HOLE rounds up to nearest block
            goto_next_block(ack)
            next
          end
          acked << sp
        rescue IO::EOFError
          break
        end
        acked.sort!
      end
    end

    # Jump to the next block in a file
    private def goto_next_block(f)
      new_pos = ((f.pos // MFile::PAGESIZE) + 1) * MFile::PAGESIZE
      f.pos = new_pos
    end

    SEEK_HOLE = 4

    # If the file has been preallocated, this truncates the file to the last allocated block
    # Truncate sparse index file, can be if not gracefully shutdown.
    # If not truncated the restore_index will create too large arrays
    private def truncate_sparse_file(f : File) : Nil
      start_pos = f.pos
      {% if flag?(:linux) %}
        # use lseek SEEK_HOLE to find unallocated blocks and truncate from there
        begin
          seek_value = LibC.lseek(f.fd, 0, SEEK_HOLE)
          raise IO::Error.from_errno "Unable to seek" if seek_value == -1
          if f.size != seek_value
            @log.info { "Truncating unallocated blocks in #{File.basename f.path} from #{f.size} to #{seek_value}" }
            f.truncate seek_value
          end
        ensure
          f.pos = start_pos
        end
      {% end %}
      # then read segment positions until we find only null bytes and truncate from there
      size = pos = start_pos
      loop do
        sp = SegmentPosition.from_io f
        if sp.zero?
          goto_next_block(f)
          pos = f.pos
        else
          pos += SP_SIZE
          size = pos
        end
      rescue IO::EOFError
        break
      end
      return if f.size == size
      @log.info { "Truncating #{File.basename f.path} from #{f.size} to #{size}" }
      f.truncate size
    ensure
      f.pos = start_pos if start_pos
    end

    def enq_file_size
      @enq.size
    end

    def ack_file_size
      @ack.size
    end

    private def ack_max_file_size
      SP_SIZE * Config.instance.queue_max_acks
    end
  end
end
