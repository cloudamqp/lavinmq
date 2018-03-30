require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    MAX_ACK_FILE_SIZE = 16 * 1024**2
    @ack : QueueFile
    @enq : QueueFile
    @durable = true

    class QueueFile < File
      include AMQP::IO
    end

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      Dir.mkdir_p @index_dir
      restore_index
      @enq = QueueFile.open(File.join(@index_dir, "enq"), "a")
      @ack = QueueFile.open(File.join(@index_dir, "ack"), "a")
    end

    private def compact_index! : Nil
      @enq.close
      QueueFile.open(File.join(@index_dir, "enq.tmp"), "w") do |f|
        unacked = @unacked.sort.each
        next_unacked = unacked.next
        @ready.each do |sp|
          while !next_unacked.nil? && next_unacked < sp
            f.write_bytes next_unacked
            next_unacked = unacked.next
          end
          f.write_bytes sp
        end
      end
      File.rename File.join(@index_dir, "enq.tmp"), File.join(@index_dir, "enq")
      @enq = QueueFile.open(File.join(@index_dir, "enq"))
      @ack.truncate
    end

    def close(deleting = false) : Nil
      @ack.close
      @enq.close
      super
    end

    def delete
      super
      Dir.children(@index_dir).each { |f| File.delete File.join(@index_dir, f) }
      Dir.rmdir @index_dir
    end

    def publish(sp : SegmentPosition, flush = false)
      @enq.write_bytes sp
      @enq.flush if flush
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      super.tap do |env|
        if no_ack && env
          @ack.write_bytes env.segment_position
          @ack.flush
          compact_index! if @ack.pos >= MAX_ACK_FILE_SIZE
        end
      end
    end

    def ack(sp : SegmentPosition)
      @ack.write_bytes sp
      @ack.flush
      compact_index! if @ack.pos >= MAX_ACK_FILE_SIZE
      super
    end

    def purge
      @enq.truncate
      @ack.truncate
      super
    end

    private def restore_index
      @log.info "Restoring index"
      @ack.seek(0, IO::Seek::Set)
      acked = Set(SegmentPosition).new(@ack.size / sizeof(SegmentPosition))
      loop do
        break if @ack.pos == @ack.size
        acked << SegmentPosition.decode @ack
      end
      @enq.seek(0, IO::Seek::Set)
      loop do
        break if @enq.pos == @enq.size
        sp = SegmentPosition.decode @enq
        @ready << sp unless acked.includes? sp
      end
      @log.info "#{message_count} messages"
    rescue Errno
      @log.debug "Index not found"
    end
  end
end
