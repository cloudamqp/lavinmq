require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    MAX_SEGMENT_SIZE = 16 * 1024**2
    @ack : QueueFile
    @enq : QueueFile
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      Dir.mkdir_p @index_dir
      restore_index
      @enq_seg = last_segment "enq"
      @ack_seg = last_segment "ack"
      @enq = QueueFile.open(File.join(@index_dir, "enq.#{@enq_seg}"), "a")
      @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
    end

    def close(deleting = false)
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
      if @enq.pos >= MAX_SEGMENT_SIZE
        @enq.close
        @enq_seg += 1
        @enq = QueueFile.open(File.join(@index_dir, "enq.#{@enq_seg}"), "a")
      end
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      super.tap do |env|
        if no_ack && env
          @ack.write_bytes env.segment_position
          @ack.flush
          if @ack.pos >= MAX_SEGMENT_SIZE
            @ack.close
            @ack_seg += 1
            @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
          end
        end
      end
    end

    def ack(sp : SegmentPosition)
      @ack.write_bytes sp
      @ack.flush
      if @ack.pos >= MAX_SEGMENT_SIZE
        @ack.close
        @ack_seg += 1
        @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
      end
      super
    end

    def purge
      @enq.close
      @ack.close
      Dir.children(@index_dir).each { |f| File.delete File.join(@index_dir, f) }
      @enq_seg = 0_u32
      @ack_seg = 0_u32
      @enq = QueueFile.open(File.join(@index_dir, "enq.#{@enq_seg}"), "a")
      @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
      super
    end

    private def last_segment(prefix) : UInt32
      segments = Dir.glob(File.join(@index_dir, "#{prefix}.*")).sort
      last_file = segments.last? || return 0_u32
      last_file[/\d+$/].to_u32
    end

    private def restore_index
      @log.info "Restoring index"
      acks = Dir.glob(File.join(@index_dir, "ack.*")).sort
      ack_sizes = acks.map { |f| File.size f }.sum
      acked = Set(SegmentPosition).new(ack_sizes / sizeof(SegmentPosition))
      acks.each do |path|
        File.open(path) do |ack|
          loop do
            break if ack.pos == ack.size
            acked << SegmentPosition.decode ack
          end
        end
      end

      enqs = Dir.glob(File.join(@index_dir, "enq.*")).sort
      enqs.each do |path|
        File.open(path) do |enq|
          loop do
            break if enq.pos == enq.size
            sp = SegmentPosition.decode enq
            @ready << sp unless acked.includes? sp
          end
        end
      end
      @log.info "#{message_count} messages"
    rescue Errno
      @log.debug "Index not found"
    end
  end
end
