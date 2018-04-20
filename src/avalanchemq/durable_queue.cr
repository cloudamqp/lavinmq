require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    MAX_ACK_FILE_SIZE = 16 * 1024**2
    @ack : File
    @enq : File
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      Dir.mkdir_p @index_dir
      @enq = File.open(File.join(@index_dir, "enq"), "a+")
      @ack = File.open(File.join(@index_dir, "ack"), "a+")
      @enq.sync = @ack.sync = true
      restore_index
    end

    private def compact_index! : Nil
      @log.debug { "Compacting index" }
      @enq.close
      File.open(File.join(@index_dir, "enq.tmp"), "w") do |f|
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
      @enq = File.open(File.join(@index_dir, "enq"))
      @enq.sync = true
      @ack.truncate
    end

    def close(deleting = false) : Nil
      @log.debug { "Closing index files" }
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
      @log.info "Purging"
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
    rescue ex : Errno
      @log.debug { "Could not restore index: #{ex.inspect}" }
    end
  end
end
