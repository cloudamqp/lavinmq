require "file_utils"
require "./queue"

module AvalancheMQ
  class DurableQueue < Queue
    MAX_ACK_FILE_SIZE = 16 * 1024**2
    @ack : File?
    @enq : File?
    @durable = true

    def initialize(@vhost : VHost, @name : String,
                   @exclusive : Bool, @auto_delete : Bool,
                   @arguments : Hash(String, AMQP::Field))
      super
      @index_dir = File.join(@vhost.data_dir, Digest::SHA1.hexdigest @name)
      @log.debug { "Index dir: #{@index_dir}" }
      Dir.mkdir_p @index_dir
      restore_index
    end

    private def compact_index! : Nil
      @log.info { "Compacting index" }
      @enq.try &.close
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
        until next_unacked == Iterator::Stop::INSTANCE
          f.write_bytes next_unacked.as(SegmentPosition)
          next_unacked = unacked.next
        end
      end
      File.rename File.join(@index_dir, "enq.tmp"), File.join(@index_dir, "enq")
      @enq = File.open(File.join(@index_dir, "enq"), "a")
      @ack ||= File.open(File.join(@index_dir, "ack"), "a")
      @ack.not_nil!.truncate
    end

    def close(deleting = false) : Nil
      @log.debug { "Closing index files" }
      @ack.try &.close
      @enq.try &.close
      super
    end

    def delete
      super
      FileUtils.rm_rf @index_dir
    end

    def publish(sp : SegmentPosition, persistent = false)
      @enq ||= File.open(File.join(@index_dir, "enq"), "a")
      @enq.not_nil!.write_bytes sp
      @enq.not_nil!.flush if persistent
      super
    end

    def get(no_ack : Bool) : Envelope | Nil
      super.tap do |env|
        if env && no_ack
          persistent = env.message.properties.delivery_mode.try { 0_u8 } == 2_u8
          @ack ||= File.open(File.join(@index_dir, "ack"), "a")
          @ack.not_nil!.write_bytes env.segment_position
          @ack.not_nil!.flush if persistent
          compact_index! if @ack.not_nil!.pos >= MAX_ACK_FILE_SIZE
        end
      end
    end

    def ack(sp : SegmentPosition, flush : Bool)
      @ack ||= File.open(File.join(@index_dir, "ack"), "a")
      @ack.not_nil!.write_bytes sp
      @ack.not_nil!.flush if flush
      compact_index! if @ack.not_nil!.pos >= MAX_ACK_FILE_SIZE
      super
    end

    def purge
      @log.info "Purging"
      @enq ||= File.open(File.join(@index_dir, "enq"), "a")
      @ack ||= File.open(File.join(@index_dir, "ack"), "a")
      @enq.not_nil!.truncate
      @ack.not_nil!.truncate
      super
    end

    private def restore_index
      @log.info "Restoring index"
      acked = Set(SegmentPosition).new(0)
      if File.exists? File.join(@index_dir, "ack")
        File.open(File.join(@index_dir, "ack")) do |ack|
          acked = Set(SegmentPosition).new(ack.size / sizeof(SegmentPosition))
          loop do
            break if ack.pos == ack.size
            acked << SegmentPosition.decode ack
          end
        end
      end
      if File.exists? File.join(@index_dir, "enq")
        File.open(File.join(@index_dir, "enq")) do |enq|
          loop do
            break if enq.pos == enq.size
            sp = SegmentPosition.decode enq
            @ready << sp unless acked.includes? sp
          end
          @log.info { "#{message_count} messages" }
        end
      end
    rescue ex : Errno
      @log.error { "Could not restore index: #{ex.inspect}" }
    end
  end
end
