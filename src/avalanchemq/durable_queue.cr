require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    MAX_ACK_FILE_SIZE = 16 * 1024**2
    @ack : QueueFile
    @enq : QueueFile
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : AMQP::Table)
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      Dir.mkdir_p @index_dir
      @enq = QueueFile.open(File.join(@index_dir, "enq"), "w")
      @ack = QueueFile.open(File.join(@index_dir, "ack"), "w")
      restore_index
    end

    private def compact_index! : Nil
      @enq.close
      QueueFile.open(File.join(@index_dir, "enq.tmp"), "w") do |f|
        unacked = @unacked.to_a.sort.each
        next_unacked = unacked.next
        @ready.each do |sp|
          while next_unacked != Iterator::Stop::INSTANCE && next_unacked.as(SegmentPosition) < sp
            f.write_bytes next_unacked.as(SegmentPosition)
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

    def publish(sp : SegmentPosition, persistent = false)
      if persistent
        @enq.write_bytes sp
        @enq.flush
      end
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      super.tap do |env|
        if env && no_ack
          persistent = env.message.properties.delivery_mode.try { 0_u8 } == 2_u8
          if persistent
            @ack.write_bytes env.segment_position
            @ack.flush
            compact_index! if @ack.pos >= MAX_ACK_FILE_SIZE
          end
        end
      end
    end

    def ack(sp : SegmentPosition, flush : Bool)
      @ack.write_bytes sp
      @ack.flush if flush
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
