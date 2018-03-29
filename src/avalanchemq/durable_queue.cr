require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    MAX_SEGMENT_SIZE = 16 * 1024**2
    @ack_seg : String
    @enq_seg : String
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
      if @enq.pos >= MAX_SEGMENT_SIZE
        @enq.close
        @enq_seg = sp.to_s
        @enq = QueueFile.open(File.join(@index_dir, "enq.#{@enq_seg}"), "a")
      end
      @enq.write_bytes sp
      @enq.flush if flush
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      super.tap do |env|
        if no_ack && env
          if @ack.pos >= MAX_SEGMENT_SIZE
            @ack.close
            @ack_seg = env.segment_position.to_s
            @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
          end
          @ack.write_bytes env.segment_position
          @ack.flush
        end
      end
    end

    def ack(sp : SegmentPosition)
      if @ack.pos >= MAX_SEGMENT_SIZE
        @ack.close
        @ack_seg = sp.to_s
        @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
      end
      @ack.write_bytes sp
      @ack.flush
      super
    end

    def purge
      @enq.close
      @ack.close
      Dir.children(@index_dir).each { |f| File.delete File.join(@index_dir, f) }
      @enq_seg = "0" * 20
      @ack_seg = "0" * 20
      @enq = QueueFile.open(File.join(@index_dir, "enq.#{@enq_seg}"), "a")
      @ack = QueueFile.open(File.join(@index_dir, "ack.#{@ack_seg}"), "a")
      super
    end

    private def last_segment(prefix) : String
      segments = Dir.glob(File.join(@index_dir, "#{prefix}.*")).sort
      last_file = segments.last? || return "0" * 20
      last_file[4, 20]
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
