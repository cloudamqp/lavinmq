require "./queue"
module AvalancheMQ
  class DurableQueue < Queue
    @ack : QueueFile
    @enq : QueueFile
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @enq = QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.enq"), "a+")
      @ack = QueueFile.open(File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.ack"), "a+")
      restore_index
    end

    def close(deleting = false)
      @ack.close
      @enq.close
      super
    end

    def delete
      super
      File.delete File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.enq")
      File.delete File.join(@vhost.data_dir, "#{Digest::SHA1.hexdigest @name}.ack")
    rescue ex : Errno
      @log.debug "File not found when deleting"
    end

    def publish(sp : SegmentPosition, flush = false)
      @enq.write_bytes sp
      @enq.flush if flush
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      sp = @ready.shift? || return nil
      if no_ack
        @ack.write_bytes sp
      else
        @unacked << sp
      end
      read(sp)
    end

    def ack(sp : SegmentPosition)
      @ack.write_bytes sp
      @ack.flush
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
