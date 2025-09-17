module LavinMQ
  module MessageStore
    class ClosedError < ::Channel::ClosedError; end

    class Error < Exception
      def initialize(mfile : MFile, cause = nil)
        super("path=#{mfile.path} pos=#{mfile.pos} size=#{mfile.size}", cause: cause)
      end
    end

    def empty?
      @size.zero?
    end

    def first?
      nil
    end

    def shift?
      nil
    end

    def delete
      @closed = true
      @empty.close
      @segments.reject! { |_, f| delete_file(f, including_meta: true); true }
      @acks.reject! { |_, f| delete_file(f); true }
      FileUtils.rm_rf @msg_dir
    end
  end
end
